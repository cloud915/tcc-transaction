package org.mengyun.tcctransaction.utils;

import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;

/**
 * Created by changming.xie on 2/23/17.
 */
public class TransactionUtils {
    // 判定 是否为合法的 事务上下文
    public static boolean isLegalTransactionContext(boolean isTransactionActive, Propagation propagation, TransactionContext transactionContext) {
        // 传播MANDATORY（非事务）&& 未开启事务 && 上下文为空
        if (propagation.equals(Propagation.MANDATORY) && !isTransactionActive && transactionContext == null) {
            return false;
        }

        return true;
    }
}
