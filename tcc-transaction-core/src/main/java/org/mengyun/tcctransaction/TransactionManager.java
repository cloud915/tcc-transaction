package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changmingxie on 10/26/15.
 *
 *
 *
 * transactionMap 记录、调整各个线程中事务对象（每个线程一个事务、包含事务状态或阶段）
 *
 * 其中TransactionConfigurator用于管理实务信息，TCC三个阶段，数据的持久化、删除等
 * TransactionConfigurator==>JdbcTransactionRepository 由TransactionManager的持有者提供、构造函数传递
 *
 * 通过new方式，直接实例化，由TransactionConfigurator持有
 */
public class TransactionManager {
    // 由TransactionManager的持有者TccTransactionConfigurator提供
    private TransactionConfigurator transactionConfigurator;

    public TransactionManager(TransactionConfigurator transactionConfigurator) {
        // 由TransactionManager的持有者TccTransactionConfigurator提供、构造函数传递
        this.transactionConfigurator = transactionConfigurator;
    }

    private final Map<Thread, Transaction> transactionMap = new ConcurrentHashMap<Thread, Transaction>();

    public void begin() {
        // 事务起点，初始化Transaction对象，设置事务的类型（根事务）、状态
        Transaction transaction = new Transaction();
        //
        transaction.setTransactionType(TransactionType.ROOT);
        // 第一阶段Trying
        transaction.setStatus(TransactionStatus.TRYING);
        // 缓存事务信息对象
        this.transactionMap.put(Thread.currentThread(), transaction);

        // 找到事务信息的仓储对象，用于持久化/记录
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        transactionRepository.create(transaction);
    }

    public void propagationNewBegin(TransactionContext transactionContext) {
        // 带有事务上下文，因此不是根事务【分支事务】
        Transaction transaction = new Transaction(transactionContext);
        // 分支事务
        transaction.setTransactionType(TransactionType.BRANCH);
        // 第一阶段Trying
        transaction.setStatus(TransactionStatus.TRYING);
        // 缓存事务信息对象
        this.transactionMap.put(Thread.currentThread(), transaction);

        transactionConfigurator.getTransactionRepository().create(transaction);
    }
    // 更新分支事务信息，与root事务的状态保持一致
    public void propagationExistBegin(TransactionContext transactionContext) {
        // 找到当前事务仓储，从中查找事务信息对象
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            // 设置事务信息对象的状态，变更为事务上下文中的状态【分支事务的状态，依赖于transactionContext的状态，也就是Root事务的状态】
            transaction.setStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            // 更新本地缓存
            this.transactionMap.put(Thread.currentThread(), transaction);
            // 更新事务仓储中的对象
            transactionRepository.update(transaction);
        }
    }


    public void commit() {
        // 获取当前线程的事务信息对象
        Transaction transaction = getCurrentTransaction();

        if (transaction != null) {
            // 设置第二阶段confirming
            transaction.setStatus(TransactionStatus.CONFIRMING);
            // 更新持久化的事务状态
            transactionConfigurator.getTransactionRepository().update(transaction);
            // 让注册进同一事务的所有参与者，都进入提交阶段【真正的操作事务状态】
            transaction.commit();
            // commit完成后，删除持久化的事务信息
            transactionConfigurator.getTransactionRepository().delete(transaction);
        }
    }

    public Transaction getCurrentTransaction() {
        return transactionMap.get(Thread.currentThread());
    }

    public void rollback() {
        // 获取当前线程的事务信息对象
        Transaction transaction = getCurrentTransaction();

        if (transaction != null) {
            // 设置第三阶段cancelling
            transaction.setStatus(TransactionStatus.CANCELLING);
            // 更新持久化的事务状态
            transactionConfigurator.getTransactionRepository().update(transaction);

            try {
                // 让注册进同一事务的所有参与者，都进入取消阶段【真正的操作事务状态】
                transaction.rollback();
                // rollback完成后，删除持久化的事务信息
                transactionConfigurator.getTransactionRepository().delete(transaction);
            } catch (Throwable rollbackException) {
                // 异常处理，如果事务类型为根事务，需要进行异常事务标记，后续job再处理
                if (transaction.getTransactionType().equals(TransactionType.ROOT)) {
                    transactionConfigurator.getTransactionRepository().addErrorTransaction(transaction);
                }
                throw new RuntimeException(rollbackException);
            }
        }
    }
}
