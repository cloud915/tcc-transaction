package org.mengyun.tcctransaction.spring.utils;

import org.mengyun.tcctransaction.MethodType;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.TransactionContext;

/**
 * Created by changmingxie on 11/21/15.
 */
public class CompensableMethodUtils {

    public static MethodType calculateMethodType( TransactionContext transactionContext, boolean isCompensable) {
        // 定义四种方法类型，根据参数、注解的不同，进行分类
        if (transactionContext == null && isCompensable) {
            // 参数列表不包含transactionContext、标注了@Compensable：定义为根事务方法
            //isRootTransactionMethod
            return MethodType.ROOT;
        } else if (transactionContext == null && !isCompensable) {
            // 参数列表不包含transactionContext、未标注@Compensable：定义为消费者方法【第二、第三阶段，以及中间其他的方法】
            //isSoaConsumer
            return MethodType.CONSUMER;
        } else if (transactionContext != null && isCompensable) {
            // 参数列表包含transactionContext、标注了@Compensable：定义为提供者方法【应该是分支事务的方法】
            //isSoaProvider
            return MethodType.PROVIDER;
        } else {
            // 其他情况：定义为普通方法
            return MethodType.NORMAL;
        }
    }

    public static int getTransactionContextParamPosition(Class<?>[] parameterTypes) {

        int i = -1;

        for (i = 0; i < parameterTypes.length; i++) {
            // 找到参数列表中，TransactionContext类型参数的索引/位置
            if (parameterTypes[i].equals(org.mengyun.tcctransaction.api.TransactionContext.class)) {
                break;
            }
        }
        return i;
    }

    public static TransactionContext getTransactionContextFromArgs(Object[] args) {

        TransactionContext transactionContext = null;
        // 遍历切面方法的参数列表，找到TransactionContext类型的参数
        for (Object arg : args) {
            if (arg != null && org.mengyun.tcctransaction.api.TransactionContext.class.isAssignableFrom(arg.getClass())) {

                transactionContext = (org.mengyun.tcctransaction.api.TransactionContext) arg;
            }
        }

        return transactionContext;
    }
}
