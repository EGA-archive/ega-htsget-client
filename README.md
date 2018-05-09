# EGA htsget Client

This is Java-based a reference implementation of a client for the GA4GH htsget API, version 1.0 (http://samtools.github.io/hts-specs/htsget.html).

This code is provided as-is. It is not being developed or supported by the EGA. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine.

### Installing

A step by step series of examples that tell you have to get a development env running

The repository contains pre-compiled jar files with the client. To build it on your local machine, run

```
ant jar
```

This will produce a set of files to run the client in the /dist folder. To create a version with all dependencies packaged into a single jar file, run

```
ant jar package-for-store
```

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.md](LICENSE.md) file for details

