if [ -z "$1" ]; then
  echo "Usage: $0 <path to protoc>"
  exit 1
fi

# for each .txtpb file in pwd:
for f in *.txtpb; do
  # extract the message type from the file
  message_type=$(grep proto-message $f | sed 's/# proto-message: //')
  # replace the .txtpb extension with .binpb to get the output file name
  outfile_name=$(echo $f | sed 's/.txtpb/.binpb/')

  echo "Encoding $f as $message_type to $outfile_name"
  # encode the file using protoc -- read it into stdin for the --encode flag
  cat $f | $1 --descriptor_set_in="../schema/warcraft.desc" --encode="$message_type" > $outfile_name
done
