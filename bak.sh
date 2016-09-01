t=`date +"%Y%m%d%H%M"`
tar zvcf "monitor$t.tar.gz" ./*
sz "monitor$t.tar.gz"
/bin/rm "monitor$t.tar.gz"
