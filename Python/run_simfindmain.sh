# =================================================================================
# DACAPO
# =================================================================================
echo "================================================================================"
cmd="zcat AVRORA/avrora.trace.gz |  ../simulator-find-main AVRORA/avrora.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > avrora-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="zcat BATIK/batik.trace.gz |  ../simulator-find-main BATIK/batik.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > batik-main.txt"
echo $cmd
eval $cmd
# cmd="zcat ECLIPSE/eclipse.trace.gz |  ../simulator-find-main ECLIPSE/eclipse.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > eclipse-main.txt"
# cmd="zcat FOP/fop.trace.gz |  ../simulator-find-main FOP/fop.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > fop-main.txt"
# cmd="zcat h2.trace.gz |  ../simulator-find-main h2.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > h2-main.txt"
echo "================================================================================"
cmd="zcat JYTHON/jython.trace.gz |  ../simulator-find-main JYTHON/jython.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > jython-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="zcat LUINDEX/luindex.trace.gz |  ../simulator-find-main LUINDEX/luindex.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > luindex-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="zcat LUSEARCH/lusearch.trace.gz |  ../simulator-find-main LUSEARCH/lusearch.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > lusearch-main.txt"
echo $cmd
eval $cmd
# cmd="zcat PMD/pmd.trace.gz |  ../simulator-find-main PMD/pmd.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > pmd-main.txt"
echo "================================================================================"
cmd="zcat SUNFLOW/sunflow.trace.gz |  ../simulator-find-main SUNFLOW/sunflow.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > sunflow-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="zcat TOMCAT/tomcat.trace.gz |  ../simulator-find-main TOMCAT/tomcat.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > tomcat-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="zcat XALAN/xalan.trace.gz |  ../simulator-find-main XALAN/xalan.names  org/dacapo/harness/TestHarness  runBenchmark  UNUSED > xalan-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
# =================================================================================
# SPECJVM
# =================================================================================
cmd="bzcat _201_COMPRESS/_201_compress.trace.bz2 |  ../simulator-find-main _201_COMPRESS/_201_compress.names  SpecApplication runBenchmark  UNUSED > _201_compress-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _202_JESS/_202_jess.trace.bz2 |  ../simulator-find-main _202_JESS/_202_jess.names  SpecApplication runBenchmark  UNUSED > _202_jess-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _205_RAYTRACE/_205_raytrace.trace.bz2 |  ../simulator-find-main _205_RAYTRACE/_205_raytrace.names  SpecApplication runBenchmark  UNUSED > _205_raytrace-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _209_DB/_209_db.trace.bz2 |  ../simulator-find-main _209_DB/_209_db.names  SpecApplication runBenchmark  UNUSED > _209_db-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _213_JAVAC/_213_javac.trace.bz2 |  ../simulator-find-main _213_JAVAC/_213_javac.names  SpecApplication runBenchmark  UNUSED > _213_javac-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _222_MPEGAUDIO/_222_mpegaudio.trace.bz2 |  ../simulator-find-main _222_MPEGAUDIO/_222_mpegaudio.names  SpecApplication runBenchmark  UNUSED > _222_mpegaudio-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _227_MTRT/_227_mtrt.trace.bz2 |  ../simulator-find-main _227_MTRT/_227_mtrt.names  SpecApplication runBenchmark  UNUSED > _227_mtrt-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
cmd="bzcat _228_JACK/_228_jack.trace.bz2 |  ../simulator-find-main _228_JACK/_228_jack.names  SpecApplication runBenchmark  UNUSED > _228_jack-main.txt"
echo $cmd
eval $cmd
echo "================================================================================"
