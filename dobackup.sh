#!/bin/bash

cd "$(dirname $(realpath "$0"))"


function set_defaults {
    prefix="backups"
    args=""
    prune="1week_3,1month_10,6month_20,1year_25"
}


function print_help {
    echo "Usage: $0 [--key=value]"
    echo
    echo "Run backup with certain arguments and prune older backsups"
    echo "The backup is performed relatvie to the $(dirname $(realpath $0))"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --prefix          name of the subdirectory to use"
    echo "                    Default: \"${prefix}\""
    echo
    echo "  --prune           rules how to prune older backups"
    echo "                    Default: \"${prune}\""
    echo
    echo "  other arguments are passed to ./cassandra_backup.sh"
    echo "  note that --dumps argument is ignored!"
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
            -h|--help)
                print_help
                exit
                ;;
            --dumps=*)
                echo "ignoring --dumps argument. specify directory via --prefix"
                shift
                ;;
            --prefix=*)
                prefix="${i#*=}"
                shift
                ;;
            --prune=*)
                prune="${i#*=}"
                shift
                ;;
            *)
                args+=" ${i}"
                shift
                ;;
        esac
    done
}


function prune {
    period="$1"
    maxnumber=$(( "$2" + 1 ))

    oldest=$(date --date="-${period}" +%s)

    find -mindepth 2 -maxdepth 2 \
         -wholename "*${prefix}*" \
         -type d -print0 | \
        while IFS= read -r -d '' dir
        do
            time=$(echo "${dir}" | sed "s|.*${prefix}/||")
            time=$(date --date="${time}" +%s)

            if [ "${time}" -gt "${oldest}" ]
            then
                echo "${dir}"
            fi
        done | sort -r | \
            tail -n +"${maxnumber}" | \
            xargs rm -rf
}


function do_backup {
    ./cassandra_backup.sh backup --dumps="${dumpsdir}" $@
}

set_defaults
parse_args $@
dumpsdir="${prefix}/$(date +%Y-%m-%dT%H:%M:%S)"

do_backup ${args} || { rm -rf "${dumpsdir}"; exit 1; }
echo "${prune}" | tr '_' ' ' | tr ',' '\0' | \
    while IFS= read -r -d '' pruneline
    do
        prune ${pruneline}
    done
