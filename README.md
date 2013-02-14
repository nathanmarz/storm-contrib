About
=====

**storm-contrib** is a community repository for modules to use with Storm. These include a variety of spouts/bolts for integrating with other systems (Redis, Kafka, MongoDB, etc), as well as code for common tasks a Storm developer encounters.

For more information about Storm itself, see [the Storm GitHub repository](http://github.com/nathanmarz/storm).

Organization
============

storm-contrib is organized as a "super-project" with a sub-folder for each module. Each module is distributed independently and module owners are responsible for distribution.

##Git Submodules

Some storm-contrib modules are git submodules (links to external github repositories). This allows storm-contrib sub-projects to be maintained externally (so those projects can maintain branches and tags independently), but also included in storm-contrib to increase community visibility.

More information about how git submodules work can be found in the [online git documentaton on submodules](http://git-scm.com/book/en/Git-Tools-Submodules).

## Initializing storm-contrib submodules

When you clone storm-contrib, the modules that are git submodules will appear as empty directories.

To initialize the git submodules use the following command:

	git submodule init

To pull down the latest versions of submodules:

	git submodule update



Contributing
============

If you're interested in contributing a module to storm-contrib, send an email to the [Storm mailing list](http://groups.google.com/group/storm-user). You will then be given commit rights to storm-contrib. The advantage of having your module be part of storm-contrib instead of your own project is more visibility for your code. However, if you'd rather maintain your module as its own project that's fine too!

## Adding your storm-related project as a git submodule

Once you have signed the contributor licence agreement and been granted commit rights to storm-contrib, you can add your project as a git submodule with the following command:

	git submodule add git://github.com/[username]/[projectname].git
