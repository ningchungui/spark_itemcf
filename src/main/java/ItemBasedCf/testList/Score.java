package ItemBasedCf.testList;

/**
 * Author: changdalin
 * Date: 2017/12/5
 * Description:
 **/
public class Score {
    private String id;
    private int s;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getS() {
        return s;
    }

    public void setS(int s) {
        this.s = s;
    }

    public Score(String id, int s) {
        this.id = id;
        this.s = s;
    }
}
