#!/usr/bin/env bash
set -Eeuo pipefail

DEFAULT_TARGET="latest-release"
IMAGE_REPOSITORY="${SWARMOTTER_IMAGE_REPOSITORY:-ghcr.io/sphildreth/swarmotter}"
GITHUB_REPO="${SWARMOTTER_GITHUB_REPO:-sphildreth/swarmotter}"
LATEST_RELEASE_API="${SWARMOTTER_LATEST_RELEASE_API:-https://api.github.com/repos/$GITHUB_REPO/releases/latest}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

DEPLOY_DIR="${SWARMOTTER_DEPLOY_DIR:-$SCRIPT_DIR}"
COMPOSE_FILE="${SWARMOTTER_COMPOSE_FILE:-$DEPLOY_DIR/compose.yml}"
ENV_FILE="${SWARMOTTER_ENV_FILE:-$DEPLOY_DIR/.env}"
TARGET_REQUEST="${SWARMOTTER_TARGET_IMAGE:-$DEFAULT_TARGET}"
TARGET_IMAGE=""
TARGET_VERSION=""
LATEST_RELEASE_TAG=""
FORCE_UPDATE="${SWARMOTTER_FORCE_UPDATE:-0}"
TARGET_ARG_SET=0
BACKUP_DIR="${SWARMOTTER_BACKUP_DIR:-$HOME/swarmotter-backups}"
SERVICE_NAME="${SWARMOTTER_COMPOSE_SERVICE:-swarmotter}"
CONTAINER_NAME="${SWARMOTTER_CONTAINER_NAME:-swarmotter}"
ROLLBACK_ON_FAILURE="${SWARMOTTER_ROLLBACK_ON_FAILURE:-1}"
SKIP_EGRESS_CHECK="${SWARMOTTER_SKIP_EGRESS_CHECK:-0}"

DOCKER=(docker)
rollback_tag=""
previous_env_image=""
env_updated=0
updated_service=0

usage() {
    cat <<EOF
Usage: $(basename "$0") [--force] [image|tag|latest-release]

Updates the SwarmOtter Docker Compose service, backs up config/state, restarts
the SwarmOtter container, and validates the upgrade. The script is intended to
run as a normal user and uses sudo for root-owned deployment files and state.

Defaults:
  target:       latest-release
  image repo:   $IMAGE_REPOSITORY
  GitHub repo:  $GITHUB_REPO
  deploy dir:   $DEPLOY_DIR
  env file:     $ENV_FILE
  compose file: $COMPOSE_FILE
  backup dir:   $BACKUP_DIR

With no argument, the script resolves the latest GitHub Release and uses the
matching $IMAGE_REPOSITORY:vX.Y.Z image. If the running container already has
that version label, the script exits without backing up or restarting. Use
--force to pull, recreate, and validate even when the installed version already
matches the latest release.

Environment overrides:
  SWARMOTTER_TARGET_IMAGE
  SWARMOTTER_FORCE_UPDATE=1
  SWARMOTTER_IMAGE_REPOSITORY
  SWARMOTTER_GITHUB_REPO
  SWARMOTTER_LATEST_RELEASE_API
  SWARMOTTER_DEPLOY_DIR
  SWARMOTTER_ENV_FILE
  SWARMOTTER_COMPOSE_FILE
  SWARMOTTER_BACKUP_DIR
  SWARMOTTER_ROLLBACK_ON_FAILURE=0
  SWARMOTTER_SKIP_EGRESS_CHECK=1
  SWARMOTTER_CONTAINER_HEALTH_URL
EOF
}

log() {
    printf '[swarmotter-update] %s\n' "$*"
}

die() {
    printf '[swarmotter-update] ERROR: %s\n' "$*" >&2
    exit 1
}

run() {
    log "+ $*"
    "$@"
}

compose() {
    "${DOCKER[@]}" compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

docker_cmd() {
    "${DOCKER[@]}" "$@"
}

resolve_path() {
    local value="$1"
    if [[ "$value" = /* ]]; then
        printf '%s\n' "$value"
    else
        printf '%s\n' "$DEPLOY_DIR/$value"
    fi
}

read_env_value() {
    local key="$1"
    local default="${2-}"
    local value

    if [[ -r "$ENV_FILE" ]]; then
        value="$(awk -v wanted="$key" '
            function trim(s) {
                sub(/^[[:space:]]+/, "", s)
                sub(/[[:space:]]+$/, "", s)
                return s
            }
            /^[[:space:]]*(#|$)/ { next }
            {
                line = $0
                sub(/\r$/, "", line)
                sub(/^[[:space:]]*export[[:space:]]+/, "", line)
                eq = index(line, "=")
                if (eq == 0) {
                    next
                }
                key = trim(substr(line, 1, eq - 1))
                if (key != wanted) {
                    next
                }
                value = trim(substr(line, eq + 1))
                if (value ~ /^".*"$/) {
                    value = substr(value, 2, length(value) - 2)
                }
                print value
                found = 1
                exit
            }
            END {
                if (!found) {
                    exit 1
                }
            }
        ' "$ENV_FILE" || true)"
    else
        value="$(sudo awk -v wanted="$key" '
            function trim(s) {
                sub(/^[[:space:]]+/, "", s)
                sub(/[[:space:]]+$/, "", s)
                return s
            }
            /^[[:space:]]*(#|$)/ { next }
            {
                line = $0
                sub(/\r$/, "", line)
                sub(/^[[:space:]]*export[[:space:]]+/, "", line)
                eq = index(line, "=")
                if (eq == 0) {
                    next
                }
                key = trim(substr(line, 1, eq - 1))
                if (key != wanted) {
                    next
                }
                value = trim(substr(line, eq + 1))
                if (value ~ /^".*"$/) {
                    value = substr(value, 2, length(value) - 2)
                }
                print value
                found = 1
                exit
            }
            END {
                if (!found) {
                    exit 1
                }
            }
        ' "$ENV_FILE" || true)"
    fi

    if [[ -n "$value" ]]; then
        printf '%s\n' "$value"
        return 0
    fi

    if (($# >= 2)); then
        printf '%s\n' "$default"
        return 0
    fi

    return 1
}

current_env_image() {
    read_env_value SWARMOTTER_IMAGE
}

set_env_image() {
    local image="$1"
    local dir base tmp
    dir="$(dirname -- "$ENV_FILE")"
    base="$(basename -- "$ENV_FILE")"
    tmp="$(sudo mktemp "$dir/.$base.XXXXXX")"

    sudo awk -v image="$image" '
        BEGIN { found = 0 }
        /^SWARMOTTER_IMAGE=/ {
            print "SWARMOTTER_IMAGE=" image
            found = 1
            next
        }
        { print }
        END {
            if (!found) {
                print "SWARMOTTER_IMAGE=" image
            }
        }
    ' "$ENV_FILE" | sudo tee "$tmp" >/dev/null

    sudo chown --reference="$ENV_FILE" "$tmp"
    sudo chmod --reference="$ENV_FILE" "$tmp"
    sudo mv "$tmp" "$ENV_FILE"
}

check_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

set_target_request() {
    local request="$1"
    if [[ "$TARGET_ARG_SET" == "1" ]]; then
        die "only one image or tag argument is supported"
    fi
    TARGET_REQUEST="$request"
    TARGET_ARG_SET=1
}

parse_args() {
    while (($# > 0)); do
        case "$1" in
            -h|--help)
                usage
                exit 0
                ;;
            --force)
                FORCE_UPDATE=1
                ;;
            --)
                shift
                if (($# > 0)); then
                    set_target_request "$1"
                    shift
                fi
                if (($# > 0)); then
                    die "unexpected extra arguments: $*"
                fi
                break
                ;;
            -*)
                die "unknown option: $1"
                ;;
            *)
                set_target_request "$1"
                ;;
        esac
        shift
    done
}

version_from_tag() {
    local tag="$1"
    printf '%s\n' "${tag#v}"
}

version_from_image_ref() {
    local ref="$1"
    local tag

    [[ "$ref" != *@sha256:* ]] || return 1
    [[ "$ref" == *:* ]] || return 1

    tag="${ref##*:}"
    tag="${tag#v}"
    if [[ "$tag" =~ ^[0-9]+[.][0-9]+[.][0-9]+ ]]; then
        printf '%s\n' "$tag"
        return
    fi

    return 1
}

latest_release_tag() {
    local response tag
    response="$(curl --max-time 20 -fsSL -H "Accept: application/vnd.github+json" "$LATEST_RELEASE_API")" \
        || die "failed to query latest release from $LATEST_RELEASE_API"
    tag="$(printf '%s\n' "$response" | sed -n 's/^[[:space:]]*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
    [[ -n "$tag" ]] || die "could not resolve latest release tag from $LATEST_RELEASE_API"
    printf '%s\n' "$tag"
}

resolve_target_image() {
    local request="$1"

    case "$request" in
        latest|latest-release|auto)
            LATEST_RELEASE_TAG="$(latest_release_tag)"
            TARGET_VERSION="$(version_from_tag "$LATEST_RELEASE_TAG")"
            TARGET_IMAGE="$IMAGE_REPOSITORY:$LATEST_RELEASE_TAG"
            ;;
        *@sha256:*|*:*)
            TARGET_IMAGE="$request"
            TARGET_VERSION="$(version_from_image_ref "$TARGET_IMAGE" || true)"
            ;;
        */*)
            TARGET_IMAGE="$request"
            TARGET_VERSION="$(version_from_image_ref "$TARGET_IMAGE" || true)"
            ;;
        *)
            TARGET_IMAGE="$IMAGE_REPOSITORY:$request"
            TARGET_VERSION="$(version_from_image_ref "$TARGET_IMAGE" || true)"
            ;;
    esac
}

container_exists() {
    docker_cmd inspect "$CONTAINER_NAME" >/dev/null 2>&1
}

running_container_image() {
    docker_cmd inspect -f '{{.Config.Image}}' "$CONTAINER_NAME" 2>/dev/null || true
}

running_container_version() {
    local version
    version="$(docker_cmd inspect -f '{{ index .Config.Labels "org.opencontainers.image.version" }}' "$CONTAINER_NAME" 2>/dev/null || true)"
    [[ "$version" != "<no value>" ]] || version=""
    printf '%s\n' "$version"
}

skip_if_current() {
    local current_image current_version

    if ! container_exists; then
        return 0
    fi

    current_image="$(running_container_image)"
    current_version="$(running_container_version)"

    if [[ "$FORCE_UPDATE" == "1" ]]; then
        log "Force enabled; continuing even if the installed version matches"
        return 0
    fi

    if [[ -n "$TARGET_VERSION" && "$current_version" == "$TARGET_VERSION" ]]; then
        log "SwarmOtter is already at version $TARGET_VERSION; nothing to do"
        exit 0
    fi

    if [[ -n "$current_image" && "$current_image" == "$TARGET_IMAGE" ]]; then
        log "SwarmOtter is already using image $TARGET_IMAGE; nothing to do"
        exit 0
    fi

    return 0
}

select_docker_command() {
    if docker info >/dev/null 2>&1; then
        DOCKER=(docker)
        return
    fi

    log "docker is not available directly; trying sudo docker"
    sudo -v
    if sudo docker info >/dev/null 2>&1; then
        DOCKER=(sudo docker)
        return
    fi

    die "docker is not accessible as $(id -un) or through sudo"
}

backup_paths() {
    local gluetun_env_file
    local swarmotter_config
    local swarmotter_state
    local gluetun_state
    local gluetun_env_path

    gluetun_env_file="$(read_env_value GLUETUN_ENV_FILE gluetun.env)"
    swarmotter_config="$(read_env_value SWARMOTTER_CONFIG /srv/swarmotter/config/swarmotter.toml)"
    swarmotter_state="$(read_env_value SWARMOTTER_STATE /srv/swarmotter/state)"
    gluetun_state="$(read_env_value GLUETUN_STATE_DIR /srv/swarmotter/gluetun)"
    gluetun_env_path="$(resolve_path "$gluetun_env_file")"

    printf '%s\0' "$ENV_FILE"
    printf '%s\0' "$COMPOSE_FILE"

    [[ -e "$gluetun_env_path" ]] && printf '%s\0' "$gluetun_env_path"
    [[ -e "$swarmotter_config" ]] && printf '%s\0' "$swarmotter_config"
    [[ -e "$swarmotter_state" ]] && printf '%s\0' "$swarmotter_state"
    [[ -e "$gluetun_state" ]] && printf '%s\0' "$gluetun_state"
}

create_backup() {
    local stamp backup_tmp backup_path manifest_path
    local -a paths
    local tar_status
    stamp="$(date -u +'%Y%m%dT%H%M%SZ')"
    backup_path="$BACKUP_DIR/swarmotter-backup-$stamp.tar.gz"
    backup_tmp="$backup_path.tmp"
    manifest_path="$backup_path.sha256"

    install -d -m 0700 "$BACKUP_DIR"
    sudo -v

    log "Creating backup at $backup_path"
    mapfile -d '' -t paths < <(backup_paths)
    ((${#paths[@]} > 0)) || die "no backup paths were found"

    umask 077
    set +e
    # The tar process runs with sudo so it can read container-owned state; the
    # redirect intentionally runs as the invoking user so the backup is theirs.
    # shellcheck disable=SC2024
    sudo tar --ignore-failed-read --warning=no-file-changed -czf - "${paths[@]}" > "$backup_tmp"
    tar_status=$?
    set -e
    if [[ "$tar_status" -gt 1 ]]; then
        rm -f "$backup_tmp"
        die "backup failed with tar exit status $tar_status"
    fi
    if [[ "$tar_status" -eq 1 ]]; then
        log "Backup completed with tar warnings; archive was still written"
    fi
    mv "$backup_tmp" "$backup_path"
    sha256sum "$backup_path" > "$manifest_path"

    log "Backup complete: $backup_path"
}

tag_rollback_image() {
    local current_image_id
    if ! docker_cmd inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
        log "No existing $CONTAINER_NAME container found; skipping rollback image tag"
        return
    fi

    current_image_id="$(docker_cmd inspect -f '{{.Image}}' "$CONTAINER_NAME")"
    rollback_tag="swarmotter:rollback-$(date -u +'%Y%m%dT%H%M%SZ')"
    run docker_cmd tag "$current_image_id" "$rollback_tag"
    log "Rollback image tag: $rollback_tag"
}

recreate_stack() {
    log "Recreating the Compose stack"
    compose down
    compose up -d
}

validate_health() {
    local port
    local url
    local container_url
    local attempt

    port="$(read_env_value SWARMOTTER_WEB_PORT 9091)"
    url="${SWARMOTTER_HEALTH_URL:-http://127.0.0.1:$port/health}"
    container_url="${SWARMOTTER_CONTAINER_HEALTH_URL:-http://127.0.0.1:9091/health}"

    log "Waiting for health endpoint: $url"
    for attempt in {1..30}; do
        if curl --max-time 5 -fsS "$url" >/dev/null; then
            log "Health endpoint passed"
            return
        fi
        if ((attempt < 30)); then
            sleep 2
        fi
    done

    log "Host health check failed; checking health inside the SwarmOtter network namespace: $container_url"
    if compose exec -T "$SERVICE_NAME" curl --max-time 5 -fsS "$container_url" >/dev/null; then
        die "health passes inside the SwarmOtter container but not through the host-published port. For Gluetun deployments, set FIREWALL_INPUT_PORTS=9091 in the Gluetun environment file."
    fi

    die "health endpoint did not pass: $url"
}

validate_container() {
    local running configured_image version revision

    running="$(docker_cmd inspect -f '{{.State.Running}}' "$CONTAINER_NAME")"
    [[ "$running" == "true" ]] || die "$CONTAINER_NAME is not running"

    configured_image="$(docker_cmd inspect -f '{{.Config.Image}}' "$CONTAINER_NAME")"
    [[ "$configured_image" == "$TARGET_IMAGE" ]] || die "$CONTAINER_NAME uses $configured_image, expected $TARGET_IMAGE"

    version="$(docker_cmd inspect -f '{{ index .Config.Labels "org.opencontainers.image.version" }}' "$CONTAINER_NAME")"
    revision="$(docker_cmd inspect -f '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$CONTAINER_NAME")"
    log "Running image label: version=$version revision=$revision"
}

validate_egress() {
    if [[ "$SKIP_EGRESS_CHECK" == "1" ]]; then
        log "Skipping contained egress check"
        return
    fi

    log "Checking outbound connectivity from inside the SwarmOtter container"
    compose exec -T "$SERVICE_NAME" curl --max-time 20 -fsS https://ifconfig.me >/tmp/swarmotter-egress-ip.txt
    log "Container egress IP: $(cat /tmp/swarmotter-egress-ip.txt)"
    rm -f /tmp/swarmotter-egress-ip.txt
}

rollback() {
    local code="$1"

    if [[ "$updated_service" == "1" && "$ROLLBACK_ON_FAILURE" == "1" && -n "$rollback_tag" ]]; then
        log "Validation failed; rolling back to $rollback_tag"
        set_env_image "$rollback_tag"
        recreate_stack || true
        log "Rollback attempted. Backup remains in $BACKUP_DIR"
        exit "$code"
    fi

    if [[ "$env_updated" == "1" && -n "$previous_env_image" ]]; then
        log "Restoring SWARMOTTER_IMAGE in $ENV_FILE to $previous_env_image"
        set_env_image "$previous_env_image" || true
    fi

    exit "$code"
}

trap 'code=$?; if [[ $code -ne 0 ]]; then rollback "$code"; fi' EXIT

main() {
    parse_args "$@"

    [[ "$(id -u)" -ne 0 ]] || die "run this as your normal user, not root"
    [[ -f "$ENV_FILE" ]] || die "missing env file: $ENV_FILE"
    [[ -f "$COMPOSE_FILE" ]] || die "missing compose file: $COMPOSE_FILE"

    check_command awk
    check_command curl
    check_command date
    check_command install
    check_command sed
    check_command sha256sum
    check_command sudo
    check_command tar

    select_docker_command
    resolve_target_image "$TARGET_REQUEST"
    previous_env_image="$(current_env_image || true)"

    log "Deploy directory: $DEPLOY_DIR"
    log "Compose file: $COMPOSE_FILE"
    log "Environment file: $ENV_FILE"
    log "Requested target: $TARGET_REQUEST"
    [[ -n "$LATEST_RELEASE_TAG" ]] && log "Latest release: $LATEST_RELEASE_TAG"
    log "Target image: $TARGET_IMAGE"
    [[ -n "$TARGET_VERSION" ]] && log "Target version: $TARGET_VERSION"
    [[ "$FORCE_UPDATE" == "1" ]] && log "Force update: enabled"

    skip_if_current
    compose config >/dev/null
    tag_rollback_image
    create_backup

    log "Updating SWARMOTTER_IMAGE in $ENV_FILE"
    set_env_image "$TARGET_IMAGE"
    env_updated=1

    log "Pulling target image"
    compose pull "$SERVICE_NAME"

    log "Restarting the Compose stack so Docker attaches networks before VPN routes are installed"
    updated_service=1
    recreate_stack

    validate_container
    validate_health
    validate_egress

    log "Upgrade complete"
    [[ -n "$rollback_tag" ]] && log "Rollback image kept locally as $rollback_tag"
    [[ -n "$previous_env_image" ]] && log "Previous env image was $previous_env_image"
}

main "$@"
