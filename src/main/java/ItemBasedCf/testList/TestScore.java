package ItemBasedCf.testList;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: changdalin
 * Date: 2017/12/5
 * Description:
 **/
public class TestScore {

    public static void main(String[] args) {
        List<List<Score>> LL = new ArrayList<>();
        List<Score> l1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            l1.add(new Score("" + i, i));
        }
        LL.add(l1);

        l1 = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            l1.add(new Score("" + i, i));
        }
        LL.add(l1);

        LL.get(0).forEach(temp -> {
            System.out.println(temp.getId() + "---" + temp.getS());
        });
        LL.get(1).forEach(temp -> {
            System.out.println(temp.getId() + "---" + temp.getS());
        });
    }
}
