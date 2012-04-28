rm -rf gen-javabean src/jvm/storm/scribe/generated
thrift7 -r --gen java:beans,hashcode,nocamel src/scribe.thrift
mv gen-javabean/storm/scribe/generated src/jvm/storm/scribe/
rm -rf gen-javabean


