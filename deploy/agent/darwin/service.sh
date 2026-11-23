#!/bin/sh
# Shared native system-launchd packaging logic; sourced by the role wrappers.
set -eu

hermes_fail() { printf '%s\n' "$*" >&2; exit 1; }
hermes_absolute() {
    case "$1" in /*) ;; *) hermes_fail "absolute path required: $1" ;; esac
    case "$1" in /|*/|*/../*|*/..|*/./*|*/.|*//*) hermes_fail "clean dedicated path required: $1" ;; esac
}
hermes_text() {
    case "$1" in *'
'*) hermes_fail 'newlines are not permitted in service arguments' ;; esac
    if printf '%s' "$1" | LC_ALL=C grep '[[:cntrl:]]' >/dev/null; then
        hermes_fail 'control characters are not permitted in plist arguments'
    fi
}
hermes_xml() {
    hermes_text "$1"
    printf '%s' "$1" | sed -e 's/\&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"
}
hermes_role() {
    case "$1" in agent|controller) HERMES_KIND=$1 ;; *) hermes_fail 'invalid service role' ;; esac
    HERMES_LABEL=org.hermes.$HERMES_KIND
    HERMES_PLIST=/Library/LaunchDaemons/$HERMES_LABEL.plist
}
# A real template is an installed package asset, not shell code or substitution input.
hermes_render() {
    user=$1; install=$2; state=$3; shift 3
    template=$HERMES_ASSETS/service.plist
    test -f "$template" || hermes_fail 'missing service.plist package asset'
    # The template holds the fixed job policy; escaped variable keys are appended.
    sed '$d' "$template"
    printf '<key>Label</key><string>%s</string>\n' "$(hermes_xml "$HERMES_LABEL")"
    printf '<key>UserName</key><string>%s</string>\n' "$(hermes_xml "$user")"
    printf '<key>WorkingDirectory</key><string>%s</string>\n' "$(hermes_xml "$state")"
    printf '<key>ProgramArguments</key><array>\n'
    for arg in "$install/hermes-$HERMES_KIND" --root "$state" --ca "$install/credentials/ca.pem" --cert "$install/credentials/cert.pem" --key "$install/credentials/key.pem" "$@"; do
        printf '<string>%s</string>\n' "$(hermes_xml "$arg")"
    done
    printf '</array></dict></plist>\n'
}
hermes_root() { test "$(id -u)" = 0 || hermes_fail 'system launchd lifecycle requires root'; }
# Root later executes the installed checker: every existing path component must
# be privileged-owned and not replaceable by the service account.
hermes_protected_path() {
    protected=$1
    while :; do
        test ! -L "$protected" || hermes_fail 'privileged installation path must not contain symlinks'
        if test -e "$protected"; then
            set -- $(stat -f '%u %Lp' "$protected")
            test "$#" = 2 || hermes_fail 'cannot inspect installation ownership'
            test "$1" = 0 || hermes_fail 'installation and ancestors must be root-owned'
            case "$2" in ''|*[!0-7]*) hermes_fail 'cannot inspect installation mode' ;; esac
            test "$((0$2 & 022))" = 0 || hermes_fail 'installation and ancestors must not be group/world writable'
        fi
        test "$protected" != / || break
        protected=${protected%/*}
        test -n "$protected" || protected=/
    done
}
hermes_managed() {
    test -f "$install/state-root" || hermes_fail 'not a managed Hermes installation'
    IFS= read -r recorded < "$install/state-root"
    test "$recorded" = "$state" || hermes_fail 'state root differs from installed root'
}
# Return 0 for registered, 1 ONLY for the exercised exact-label absence.
# Any unqualified diagnostic is an unknown service state, never absence.
hermes_registered() {
    if diagnostic=$(LC_ALL=C launchctl print "system/$HERMES_LABEL" 2>&1 >/dev/null); then
        return 0
    else
        print_status=$?
    fi
    absent=$(printf 'Bad request.\nCould not find service "%s" in domain for system' "$HERMES_LABEL")
    if test "$print_status" = 113 && test "$diagnostic" = "$absent"; then
        return 1
    fi
    hermes_fail "service registration unknown (launchctl print status $print_status); operation incomplete: $diagnostic"
}
hermes_registration() {
    if hermes_registered; then
        launchctl bootout "system/$HERMES_LABEL" || hermes_fail 'bootout failed; service stop incomplete'
    fi
    if hermes_registered; then
        hermes_fail 'service remains registered; stop incomplete'
    fi
}
hermes_stop() {
    launchctl disable "system/$HERMES_LABEL" || hermes_fail 'disable failed; stop incomplete'
    hermes_registration
}
hermes_main() {
    hermes_role "$1"; operation=$2; shift 2
    case "$operation" in
    install)
        test "$#" -ge 6 || hermes_fail 'usage: sh install.sh USER INSTALL_DIR STATE_ROOT CA CERT KEY [entrypoint arguments...]'
        user=$1; install=$2; state=$3; ca=$4; cert=$5; key=$6; shift 6
        for value in "$user" "$install" "$state" "$ca" "$cert" "$key" "$@"; do hermes_text "$value"; done
        hermes_absolute "$install"; hermes_absolute "$state"
        test ! -L "$install" && test ! -L "$state" || hermes_fail 'install/state roots must not be symlinks'
        case "$state/" in "$install/"*) hermes_fail 'state must be outside installation directory' ;; esac
        case "$install/" in "$state/"*) hermes_fail 'installation must be outside state directory' ;; esac
        for arg in "$@"; do
            case "$arg" in -root|-root=*|--root|--root=*|-ca|-ca=*|--ca|--ca=*|-cert|-cert=*|--cert|--cert=*|-key|-key=*|--key|--key=*) hermes_fail 'root and credential flags are managed by installer' ;; esac
        done
        case "$user" in ""|-*|*[!A-Za-z0-9_-]*) hermes_fail 'invalid service account name' ;; esac
        hermes_root
        install_parent=${install%/*}; test -n "$install_parent" || install_parent=/
        state_parent=${state%/*}; test -n "$state_parent" || state_parent=/
        test -d "$install_parent" || hermes_fail 'installation parent must already be provisioned and protected'
        test -d "$state_parent" || hermes_fail 'state parent must already be provisioned and traversable by service account'
        hermes_protected_path "$install"
        uid=$(id -u "$user") || hermes_fail 'service account must already exist'
        test "$uid" != 0 || hermes_fail 'service account must be non-root'
        for file in "$HERMES_ASSETS/hermes-$HERMES_KIND" "$ca" "$cert" "$key"; do test -f "$file" || hermes_fail "missing regular file: $file"; done
        # Stage only in the dedicated install filesystem; never source configuration.
        if test -d "$install"; then
            if test -f "$install/state-root"; then
                hermes_protected_path "$install/hermes-$HERMES_KIND"
                hermes_protected_path "$install/state-root"
                hermes_protected_path "$HERMES_PLIST"
                hermes_managed
                cmp -s "$HERMES_ASSETS/hermes-$HERMES_KIND" "$install/hermes-$HERMES_KIND" && cmp -s "$ca" "$install/credentials/ca.pem" && cmp -s "$cert" "$install/credentials/cert.pem" && cmp -s "$key" "$install/credentials/key.pem" || hermes_fail 'installation differs; use verified uninstall before replacement'
                temporary=$(mktemp "$install/.plist.XXXXXX")
                hermes_render "$user" "$install" "$state" "$@" > "$temporary"
                if ! cmp -s "$temporary" "$HERMES_PLIST"; then rm -f "$temporary"; hermes_fail 'service configuration differs; use verified uninstall first'; fi
                rm -f "$temporary"
                if hermes_registered; then :; fi
                printf '%s\n' 'already installed; current stop/enable intent unchanged'
                return
            fi
            test -z "$(ls -A "$install")" || hermes_fail 'installation directory is not empty or managed'
        fi
        test ! -e "$HERMES_PLIST" || hermes_fail 'service plist already exists; refusing replacement'
        if hermes_registered; then hermes_fail 'service label already registered'; fi
        umask 077
        if test ! -d "$install"; then mkdir "$install"; fi
        mkdir "$install/credentials"
        if test ! -d "$state"; then mkdir "$state"; fi
        chown root:wheel "$install"
        chmod 0755 "$install"; chmod 0700 "$install/credentials" "$state"
        chown "$user" "$state" "$install/credentials"
        hermes_protected_path "$install"
        cp "$HERMES_ASSETS/hermes-$HERMES_KIND" "$install/hermes-$HERMES_KIND"
        chown root:wheel "$install/hermes-$HERMES_KIND"
        chmod 0755 "$install/hermes-$HERMES_KIND"
        cp "$ca" "$install/credentials/ca.pem"; cp "$cert" "$install/credentials/cert.pem"; cp "$key" "$install/credentials/key.pem"
        chown "$user" "$install/credentials/ca.pem" "$install/credentials/cert.pem" "$install/credentials/key.pem"
        chmod 0600 "$install/credentials/ca.pem" "$install/credentials/cert.pem" "$install/credentials/key.pem"
        temporary=$(mktemp "$install/.plist.XXXXXX")
        hermes_render "$user" "$install" "$state" "$@" > "$temporary"
        plutil -lint "$temporary" >/dev/null || hermes_fail 'invalid generated plist; installation incomplete'
        cp "$temporary" "$HERMES_PLIST"; rm -f "$temporary"
        chown root:wheel "$HERMES_PLIST"; chmod 0644 "$HERMES_PLIST"
        printf '%s\n' "$state" > "$install/state-root"
        chown root:wheel "$install/state-root"; chmod 0644 "$install/state-root"
        hermes_protected_path "$install/hermes-$HERMES_KIND"
        hermes_protected_path "$install/state-root"
        hermes_protected_path "$HERMES_PLIST"
        launchctl enable "system/$HERMES_LABEL"
        launchctl bootstrap system "$HERMES_PLIST"
        ;;
    start|stop|uninstall)
        test "$#" = 2 || hermes_fail 'usage: sh lifecycle.sh start|stop INSTALL_DIR STATE_ROOT (or sh uninstall.sh INSTALL_DIR STATE_ROOT)'
        install=$1; state=$2
        hermes_absolute "$install"; hermes_absolute "$state"; hermes_root
        if test "$operation" = uninstall && test ! -e "$install/state-root" && test ! -e "$HERMES_PLIST"; then
            if hermes_registered; then hermes_fail 'unmanaged registered service; uninstall incomplete'; fi
            for file in "$install/hermes-$HERMES_KIND" "$install/credentials/ca.pem" "$install/credentials/cert.pem" "$install/credentials/key.pem"; do
                test ! -e "$file" && test ! -L "$file" || hermes_fail 'managed files remain without marker; uninstall incomplete'
            done
            printf '%s\n' 'already uninstalled; state preserved'; return
        fi
        hermes_protected_path "$install"
        hermes_protected_path "$install/hermes-$HERMES_KIND"
        hermes_protected_path "$install/state-root"
        hermes_managed
        test -f "$HERMES_PLIST" || hermes_fail 'managed plist missing; manual recovery required'
        hermes_protected_path "$HERMES_PLIST"
        installed_binary=$(plutil -extract ProgramArguments.0 raw -o - "$HERMES_PLIST")
        test "$installed_binary" = "$install/hermes-$HERMES_KIND" || hermes_fail 'plist does not belong to this installation'
        if test "$operation" = start; then
            if hermes_registered; then
                launchctl enable "system/$HERMES_LABEL"
            else
                launchctl enable "system/$HERMES_LABEL"
                launchctl bootstrap system "$HERMES_PLIST"
            fi
            return
        fi
        hermes_stop
        if test "$operation" = stop; then printf '%s\n' 'disabled and unregistered; leaf quiescence not implied'; return; fi
        "$install/hermes-$HERMES_KIND" check-quiescence --root "$state" || hermes_fail 'quiescence unknown/busy; uninstall incomplete, binary, credentials and state preserved'
        rm -f "$install/hermes-$HERMES_KIND" "$install/credentials/ca.pem" "$install/credentials/cert.pem" "$install/credentials/key.pem" "$install/state-root" "$HERMES_PLIST"
        rmdir "$install/credentials" 2>/dev/null || :
        rmdir "$install" 2>/dev/null || :
        printf '%s\n' 'uninstalled; state and result bytes preserved; controller ownership unchanged'
        ;;
    *) hermes_fail 'unsupported lifecycle operation' ;;
    esac
}
