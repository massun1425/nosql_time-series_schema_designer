file=$1
echo $file
bundle exec stackprof $file --flamegraph > $(basename $file)
bundle exec stackprof $file --flamegraph-viewer $(basename $file)
