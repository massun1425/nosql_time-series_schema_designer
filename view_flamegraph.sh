file=$1
echo $file
bundle exec stackprof $file --flamegraph > $(basename $file)_flamegraph.html
bundle exec stackprof $file --flamegraph-viewer $(basename $file)_flamegraph.html

