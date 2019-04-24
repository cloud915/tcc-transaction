

package org.mengyun.tcctransaction;

/**
 * Created by changmingxie on 11/15/15.
 */
public enum TransactionType {

    ROOT(1), // 根事务
    BRANCH(2);// 分支事务

    int id;

    TransactionType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public  static TransactionType  valueOf(int id) {
        switch (id) {
            case 1:
                return ROOT;
            case 2:
                return BRANCH;
            default:
                return null;
        }
    }

}
