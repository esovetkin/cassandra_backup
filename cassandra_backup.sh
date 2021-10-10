#!/bin/bash


function set_defaults {
    if [ -z "${keyspaces}" ]
    then
        keyspaces="ipfs_files__pvgrip_storage"
        keyspaces+=" cassandra_spatial_datasets"
        keyspaces+=" cassandra_spatial_index_hash_lengths_2_7"
    fi
    host="lupin.hs_pvgrip.hs"
    dumps="./dumps"
    what="list_keyspaces"
    request_timeout=3600
    connect_timeout=600
    chunksize=2
    numprocesses=1
    maxattempts=20
}


function print_help {
    echo "Usage: $0 [backup,restore,list_keyspaces] [--key=value]"
    echo
    echo "Backup and restore small cassandra datasets"
    echo "small is less than 2e+6 entries"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --host            cassandra host to use"
    echo "                    Default: \"${host}\""
    echo
    echo "  --keyspaces       what keyspaces to backup/restore"
    echo "                    space separated names."
    echo "                    Can be given as environmental variable 'keyspaces'"
    echo "                    Default: \"${keyspaces}\""
    echo
    echo "  --dumps           directory for dumps"
    echo "                    Default: \"${dumps}\""
    echo
    echo "  --request-timeout,--connect-timeout"
    echo "                    as in cqlsh. see:"
    echo "                    https://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cqlsh_commands/cqlsh.html"
    echo "                    Defaults:"
    echo "                    --request-timeout=${request_timeout}"
    echo "                    --connect-timeout=${connect_timeout}"
    echo
    echo "  --chunksize,--numprocesses,--maxattempts"
    echo "                    corresponding arguments of COPY"
    echo "                    see: https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlshCopy.html"
    echo "                    Defaults:"
    echo "                    --chunksize=${chunksize}"
    echo "                    --numprocesses=${numprocesses}"
    echo "                    --maxattempts=${maxattempts}"
    echo
    echo "  list_keyspaces    list available keyspaces"
    echo
    echo "  backup            create backup in the --dumps directiory"
    echo "                    Dumps are placed in following files:"
    echo "                      ${dumps}/keyspace/keyspace.cqlsh"
    echo "                      ${dumps}/keyspace/table1.csv.gz"
    echo "                      ${dumps}/keyspace/table2.csv.gz"
    echo "                      ..."
    echo "                    create.cqlsh recreates the keyspace"
    echo "                    If any output files exit, backup for this file is ignored"
    echo
    echo "  restore           restore tables from the --dumps directory"
    echo "                    Note, the keyspaces are deduced from the --dumps directory,"
    echo "                    and --heyspaces argument is ignored"
    echo
    echo "Examples:"
    echo
    echo "$0 backup  --host=host1.wg"
    echo "$0 restore --host=host2.wg"
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
            --host=*)
                host="${i#*=}"
                shift
                ;;
            --keyspaces=*)
                keyspaces="${i#*=}"
                shift
                ;;
            --dumps=*)
                dumps="${i#*=}"
                shift
                ;;
            --request-timeout=*)
                request_timeout="${i#*=}"
                shift
                ;;
            --connect-timeout=*)
                connect_timeout="${i#*=}"
                shift
                ;;
            --chunksize=*)
                chunksize="${i#*=}"
                shift
                ;;
            --numprocesses=*)
                numprocesses="${i#*=}"
                shift
                ;;
            --maxattempts=*)
                maxattempts="${i#*=}"
                shift
                ;;
            backup)
                what="backup"
                ;;
            restore)
                what="restore"
                ;;
            list_keyspaces)
                what="list_keyspaces"
                ;;
            -h|--help)
                print_help
                exit
                ;;
            *)
                echo "unknown argument!"
                exit
                ;;
        esac
    done
}

function list_tables {
    keyspace="$1"
    cqlsh "${host}" -e "USE ${keyspace}; DESCRIBE tables;" | xargs || exit 1
}


function list_keyspaces {
    cqlsh "${host}" -e "DESCRIBE keyspaces;" | xargs || exit 1
}


function get_create {
    keyspace="$1"
    ofn="$2"
    cqlsh "${host}" -e "DESCRIBE keyspace ${keyspace};" > "${ofn}" || exit 1
}


function call_create {
    cqlsh "${host}" -f "$1" || exit 1
}


function copy_to {
    keyspace="$1"
    table="$2"
    ofn="$3"
    cmd="USE ${keyspace}; "
    cmd+="COPY ${table} TO STDOUT WITH HEADER=TRUE "
    cmd+="AND NUMPROCESSES=${numprocesses} "
    cmd+="AND MAXATTEMPTS=${maxattempts};"
    cqlsh \
        --request-timeout=${request_timeout} \
        --connect-timeout=${connect_timeout} \
        -e "${cmd}" \
        "${host}" | gzip > "${ofn}" || exit 1
}


function copy_from {
    keyspace="$1"
    table="$2"
    ifn="$3"
    cmd="USE ${keyspace}; "
    cmd+="COPY ${table} FROM STDIN WITH HEADER=TRUE "
    cmd+="AND CHUNKSIZE=${chunksize} "
    cmd+="AND NUMPROCESSES=${numprocesses} "
    cmd+="AND MAXATTEMPTS=${maxattempts};"
    zcat "${ifn}" | \
        cqlsh \
            --request-timeout=${request_timeout} \
            --connect-timeout=${connect_timeout} \
            -e "${cmd}" \
            "${host}" || exit 1

}


function do_backup {
    for keyspace in ${keyspaces}
    do
        odir="${dumps}/${keyspace}"
        mkdir -p "${odir}"

        ofn="${odir}/${keyspace}.cqlsh"
        if [ ! -f "${ofn}" ]
        then
            echo "getting create script for ${keyspace}"
            get_create "${keyspace}" "${ofn}"
        else
            echo "file ${ofn} exist! ignoring action"
        fi

        for table in $(list_tables "${keyspace}")
        do
            ofn="${odir}/${table}.csv.gz"
            if [ ! -f "${ofn}" ]
            then
                echo "copying ${keyspace}.${table}..."
                copy_to "${keyspace}" "${table}" "${ofn}"
            else
                echo "file ${ofn} exist! ignoring action"
            fi
        done
    done
}


function restore_keyspace {
    idir="$1"
    keyspace=$(basename "${idir}")

    ifn="${idir}/${keyspace}.cqlsh"
    if [ ! -f "${ifn}" ]
    then
        echo "${ifn} is missing! Do not restore ${keyspace}"
        return
    fi
    call_create "${ifn}"

    find "${idir}" -type f -name '*.csv.gz' -print0 | \
        while IFS= read -r -d '' dump_fn
        do
            table=$(basename "${dump_fn}")
            table=${table%*.csv.gz}
            echo "Restoring table ${table} from ${dump_fn}"
            copy_from "${keyspace}" "${table}" "${dump_fn}"
        done
}


function do_restore {
    find "${dumps}" -mindepth 1 -maxdepth 1 -type d -print0 | \
        while IFS= read -r -d '' keyspace
        do
            echo "Restoring keyspace from ${keyspace}"
            restore_keyspace "${keyspace}"
        done
}

set_defaults
parse_args $@

case "${what}" in
    backup)
        do_backup
        exit
        ;;
    restore)
        do_restore
        exit
        ;;
    list_keyspaces)
        list_keyspaces
        exit
        ;;
esac
