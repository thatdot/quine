if [ -z "$1" ]; then
  echo "Usage: $0 <path to protoc>"
  exit 1
fi

$1 --descriptor_set_out=warcraft.desc azeroth.proto argus.proto zone_rework.proto
