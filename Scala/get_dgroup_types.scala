/* Get death group types
 */
object GetDgroupTypes {

    def main(args: Array[String]) {
        val dgroup: List[Int] = get_input()
        for (rec <- dgroup) {
            println(rec)
        }
    }

    def get_input() : List[Int] = {
        var seq: List[Int] = List.empty
        for (ln <- io.Source.stdin.getLines(); if (ln.head != '-')) {
            val recs: Array[String] = ln.split(',')
            for (rec <- recs) {
                 seq = rec.toInt :: seq
            }
        }
        return seq
    }

    def get_objectinfo() : HashMap[Int] = {
        var seq: List[Int] = List.empty
        for (ln <- io.Source.stdin.getLines(); if (ln.head != '-')) {
            val recs: Array[String] = ln.split(',')
            for (rec <- recs) {
                 seq = rec.toInt :: seq
            }
        }
        return seq
    }
}
