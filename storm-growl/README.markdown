## Storm-growl

storm-growl provides a bolt implementation for growl notification.
It receive "title" and "message" from previous bolt/spout and send it to growl.

## Growl

http://growl.info/

## libgrowl

storm-growl requires libgrowl. You can download it from  http://sourceforge.net/projects/libgrowl/

## Configuration

You must set up growl can "Listen for incoming notifications" at "Growl" in "System Preferences". (for Mac users)

## Build

There is no repos for libgrowl, so please download the jar and put it under class_path or Maven Install.

## Run example

You can run TestGrowlTopology with Maven like so:
```
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.growl.TestGrowlTopology
```

You can package a jar suitable for submitting to storm with this command:
```
mvn package
```

## For old versions

You can get storm-growl for old storm versions. https://github.com/tjun/storm-growl
