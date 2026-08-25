#!/bin/sh

private_swarm_failure() {
    printf '%s\n' 'private swarm startup rejected' >&2
    return 1
}

validate_swarm_key() {
    swarm_key_path=$1

    [ -r "$swarm_key_path" ] || return 1
    [ "$(wc -c < "$swarm_key_path" | tr -d '[:space:]')" -eq 96 ] || return 1
    [ "$(wc -l < "$swarm_key_path" | tr -d '[:space:]')" -eq 3 ] || return 1

    {
        IFS= read -r first_line || return 1
        IFS= read -r second_line || return 1
        IFS= read -r third_line || return 1
    } < "$swarm_key_path"

    [ "$first_line" = "/key/swarm/psk/1.0.0/" ] || return 1
    [ "$second_line" = "/base16/" ] || return 1
    [ "${#third_line}" -eq 64 ] || return 1
    case "$third_line" in
        *[!0123456789abcdef]*) return 1 ;;
    esac
}

redact_swarm_fingerprint() {
    while IFS= read -r log_line || [ -n "$log_line" ]; do
        case "$log_line" in
            "Swarm key fingerprint: "*)
                fingerprint=${log_line#Swarm key fingerprint: }
                if [ "${#fingerprint}" -eq 32 ]; then
                    case "$fingerprint" in
                        [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f])
                            printf '%s\n' 'Swarm key fingerprint: [redacted]'
                            continue
                            ;;
                    esac
                fi
                ;;
        esac
        printf '%s\n' "$log_line"
    done
}

apply_private_swarm_config() {
    ipfs config Addresses.API "/ip4/0.0.0.0/tcp/5001" >/dev/null 2>&1 &&
        ipfs config Addresses.Gateway "/ip4/0.0.0.0/tcp/8080" >/dev/null 2>&1 &&
        ipfs config Addresses.Swarm --json '["/ip4/0.0.0.0/tcp/4001"]' >/dev/null 2>&1 &&
        ipfs config Swarm.AddrFilters --json '[]' >/dev/null 2>&1 &&
        ipfs config Bootstrap --json '[]' >/dev/null 2>&1 &&
        ipfs config Routing.Type none >/dev/null 2>&1 &&
        ipfs config AutoConf.Enabled --json false >/dev/null 2>&1 &&
        ipfs config Discovery.MDNS.Enabled --json false >/dev/null 2>&1 &&
        ipfs config Gateway.NoDNSLink --json true >/dev/null 2>&1 &&
        ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Origin --json '["*"]' >/dev/null 2>&1 &&
        ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","HEAD","OPTIONS"]' >/dev/null 2>&1 &&
        ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Headers --json '["Range","Content-Type"]' >/dev/null 2>&1 &&
        ipfs config Gateway.HTTPHeaders.Cache-Control --json '["public, max-age=29030400, immutable"]' >/dev/null 2>&1 &&
        ipfs config API.HTTPHeaders.Access-Control-Allow-Origin --json '["*"]' >/dev/null 2>&1 &&
        ipfs config API.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","POST","OPTIONS"]' >/dev/null 2>&1 &&
        ipfs config API.HTTPHeaders.Access-Control-Allow-Headers --json '["Authorization","Content-Type"]' >/dev/null 2>&1 &&
        ipfs config Datastore.BloomFilterSize --json 0 >/dev/null 2>&1
}

prepare_private_swarm() {
    IPFS_PATH=${IPFS_PATH:-/data/ipfs}
    export IPFS_PATH
    export IPFS_TELEMETRY=off

    [ "${LIBP2P_FORCE_PNET-}" = "1" ] || return 1
    case "${IPFS_SWARM_KEY_FILE-}" in
        /run/secrets/*) ;;
        *) return 1 ;;
    esac
    validate_swarm_key "$IPFS_SWARM_KEY_FILE" || return 1
    mkdir -p "$IPFS_PATH" >/dev/null 2>&1 || return 1

    if [ ! -f "$IPFS_PATH/config" ]; then
        ipfs init --empty-repo --profile=server >/dev/null 2>&1 || return 1
    fi

    umask 077
    rm -f "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    cp "$IPFS_SWARM_KEY_FILE" "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    chmod 0400 "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    apply_private_swarm_config
}

daemon_pid=
filter_pid=
fifo_path=
wait_interrupted=0
daemon_started=0
fifo_cleanup_status=0

cleanup_fifo() {
    fifo_cleanup_status=0
    if [ -n "$fifo_path" ]; then
        rm -f "$fifo_path" >/dev/null 2>&1
        fifo_cleanup_status=$?
        if [ "$fifo_cleanup_status" -eq 0 ]; then
            fifo_path=
        fi
        return "$fifo_cleanup_status"
    fi
    return 0
}

cleanup_runtime() {
    cleanup_exit_status=$?

    trap - 0 TERM INT HUP
    if [ -n "$daemon_pid" ]; then
        kill -TERM "$daemon_pid" >/dev/null 2>&1 || :
        wait_for_pid "$daemon_pid" || :
        daemon_pid=
    fi
    if [ -n "$filter_pid" ]; then
        wait_for_pid "$filter_pid" || :
        filter_pid=
    fi
    cleanup_fifo || :
    return "$cleanup_exit_status"
}

forward_term() {
    wait_interrupted=1
    if [ -n "$daemon_pid" ]; then
        kill -TERM "$daemon_pid" >/dev/null 2>&1 || :
    fi
    return 0
}

forward_int() {
    wait_interrupted=1
    if [ -n "$daemon_pid" ]; then
        kill -INT "$daemon_pid" >/dev/null 2>&1 || :
    fi
    return 0
}

forward_hup() {
    wait_interrupted=1
    if [ -n "$daemon_pid" ]; then
        kill -HUP "$daemon_pid" >/dev/null 2>&1 || :
    fi
    return 0
}

wait_for_pid() {
    child_pid=$1

    while :; do
        wait_interrupted=0
        set +e
        wait "$child_pid"
        child_status=$?
        if [ "$wait_interrupted" -eq 1 ] && [ "$child_status" -gt 128 ]; then
            continue
        fi
        return "$child_status"
    done
}

select_supervisor_exit() {
    daemon_status=$1
    filter_status=$2
    fifo_cleanup_status=$3

    if [ "$daemon_status" -ne 0 ]; then
        return "$daemon_status"
    fi
    if [ "$filter_status" -ne 0 ] || [ "$fifo_cleanup_status" -ne 0 ]; then
        return 1
    fi
    return 0
}

supervise_daemon() {
    daemon_started=0
    fifo_path="$IPFS_PATH/.private-swarm-daemon.fifo"
    cleanup_fifo
    fifo_setup_status=$?
    if [ "$fifo_setup_status" -ne 0 ]; then
        return 1
    fi

    fifo_path="$IPFS_PATH/.private-swarm-daemon.fifo"
    mkfifo "$fifo_path" >/dev/null 2>&1
    fifo_setup_status=$?
    if [ "$fifo_setup_status" -ne 0 ]; then
        cleanup_fifo || :
        return 1
    fi

    redact_swarm_fingerprint < "$fifo_path" &
    filter_pid=$!
    ipfs daemon --migrate=true --enable-gc=false > "$fifo_path" 2>&1 &
    daemon_pid=$!
    daemon_started=1

    wait_for_pid "$daemon_pid"
    daemon_status=$?
    daemon_pid=
    wait_for_pid "$filter_pid"
    filter_status=$?
    filter_pid=
    cleanup_fifo
    fifo_cleanup_status=$?
    select_supervisor_exit "$daemon_status" "$filter_status" "$fifo_cleanup_status"
}

main() {
    trap 'cleanup_runtime' 0
    trap 'forward_term' TERM
    trap 'forward_int' INT
    trap 'forward_hup' HUP

    prepare_private_swarm || {
        private_swarm_failure
        return 1
    }
    supervise_daemon
    runtime_status=$?
    if [ "$daemon_started" -eq 0 ] && [ "$runtime_status" -ne 0 ]; then
        private_swarm_failure
    fi
    trap - 0 TERM INT HUP
    return "$runtime_status"
}

if [ "$(basename "$0")" = "private-swarm-entrypoint.sh" ]; then
    main "$@"
fi
