#!/bin/bash
set -e
FILE=/opt/filezilla-server/etc/settings.xml
sed -i 's|<do_not_override_host_if_peer_is_local>false</do_not_override_host_if_peer_is_local>|<do_not_override_host_if_peer_is_local>true</do_not_override_host_if_peer_is_local>|' "$FILE"
echo "--- verifying ---"
grep "do_not_override_host_if_peer_is_local" "$FILE"
echo "--- restarting filezilla-server ---"
systemctl restart filezilla-server
echo "--- done ---"
