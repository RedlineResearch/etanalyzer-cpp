package net.veroy.analysis.etracks;


public class EdgeRecord {
    private int _srcId;
    private int _tgtId;
    private int _fieldId;
    private int _atime;
    private int _dtime;

    public EdgeRecord( int srcId,
                       int tgtId,
                       int fieldId,
                       int atime,
                       int dtime ) {
        super();
        this._srcId = srcId;
        this._tgtId = tgtId;
        this._fieldId = fieldId;
        this._atime = atime;
        this._dtime = dtime;
    }

    public EdgeRecord() {
        super();
        this._srcId = 0;
        this._tgtId = 0;
        this._fieldId = 0;
        this._atime = 0;
        this._dtime = 0;
    }

    public int hashCode() {
        int result = (int) (_srcId ^ (_srcId >>> 32));
        result = (31 * result) + (int) (_tgtId ^ (_tgtId >>> 32));
        return ((31 * result) + (int) (_atime ^ (_atime >>> 32)));
    }

    public boolean equals(Object obj) {
        if (obj instanceof EdgeRecord) {
            EdgeRecord other = (EdgeRecord) obj;
            return ( (_srcId == other.get_srcId()) &&
                     (_tgtId == other.get_tgtId()) && 
                     (_atime == other.get_atime()) );
        }
        return false;
    }

    public int get_tgtId() {
        return _tgtId;
    }
    public void set_tgtId(int tgtId) {
        this._tgtId = tgtId;
    }
    public int get_srcId() {
        return _srcId;
    }
    public void set_srcId(int srcId) {
        this._srcId = srcId;
    }
    public int get_fieldId() {
        return _fieldId;
    }
    public void set_fieldId(int fieldId) {
        this._fieldId = fieldId;
    }
    public int get_atime() {
        return _atime;
    }
    public void set_atime(int atime) {
        this._atime = atime;
    }
    public int get_dtime() {
        return _dtime;
    }
    public void set_dtime(int dtime) {
        this._dtime = dtime;
    }
}
