# Fast DDS generated type regeneration

The canonical input is `autonomy_pipeline.idl`.

The benchmark pins Fast DDS 3.4.x and therefore generates its checked-in C++
type support with Fast DDS-Gen 4.2.0 under JDK 17. Generated sources are kept in
this package so ordinary Bazel builds do not download Gradle, Maven artifacts,
or depend on a host-installed `fastddsgen`.

Expected command from this directory:

```sh
fastddsgen -replace -no-typeobjectsupport -d generated autonomy_pipeline.idl
```

Before updating generated files, record the exact generator release, Java
version, command line, and resulting source hashes in the benchmark change.
The currently checked-in support was mechanically reproduced from Fast DDS-Gen
4.2.0 templates because the Gradle distribution download timed out; it must be
replaced only after a pinned JDK 17 regeneration produces a reviewed diff.
Do not run the generator in a measured benchmark campaign.
