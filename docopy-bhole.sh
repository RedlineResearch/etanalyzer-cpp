#!/bin/bash
cp -vf _201_COMPRESS/_201_compress-DGROUPS.csv _205_RAYTRACE/_205_raytrace-DGROUPS.csv \
    _209_DB/_209_db-DGROUPS.csv \
    AVRORA/avrora-DGROUPS.csv LUINDEX/luindex-DGROUPS.csv \
    FOP/fop-DGROUPS.csv \
    SPECJBB/specjbb-DGROUPS.csv \
    XALAN/xalan-DGROUPS.csv  \
    /h/rveroy/src/data-ismm-2016/Death-groups
cp -vf _201_COMPRESS/_201_compress-DGROUPS-BY-TYPE.csv _205_RAYTRACE/_205_raytrace-DGROUPS-BY-TYPE.csv \
    _209_DB/_209_db-DGROUPS-BY-TYPE.csv \
    AVRORA/avrora-DGROUPS-BY-TYPE.csv LUINDEX/luindex-DGROUPS-BY-TYPE.csv \
    FOP/fop-DGROUPS-BY-TYPE.csv \
    SPECJBB/specjbb-DGROUPS-BY-TYPE.csv \
    XALAN/xalan-DGROUPS-BY-TYPE.csv  \
    /h/rveroy/src/data-ismm-2016/Death-groups/z-BY-TYPE
