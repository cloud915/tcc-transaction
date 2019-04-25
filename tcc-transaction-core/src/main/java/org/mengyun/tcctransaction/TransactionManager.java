package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.support.TransactionConfigurator;

/**
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {


    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());

    private TransactionConfigurator transactionConfigurator;

    public TransactionManager(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }

    private ThreadLocal<Transaction> threadLocalTransaction = new ThreadLocal<Transaction>();

    public void begin() {
        // 创建事务消息对象
        Transaction transaction = new Transaction(TransactionType.ROOT);
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        // 持久化事务消息对象、本地缓存
        transactionRepository.create(transaction);
        threadLocalTransaction.set(transaction);
    }

    public void propagationNewBegin(TransactionContext transactionContext) {
        // 依赖上下文，创建事务信息，状态与root事务保持一致
        Transaction transaction = new Transaction(transactionContext);
        transactionConfigurator.getTransactionRepository().create(transaction);

        threadLocalTransaction.set(transaction);
    }

    public void propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        // confirm或cancel阶段，才会调用，事务信息对象从仓储中拿
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            // 调整分支事务的信息对象状态，
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            // 更新本地缓存
            threadLocalTransaction.set(transaction);
        } else {
            throw new NoExistedTransactionException();
        }
    }

    public void commit() {

        // 标记事务状态为提交中
        Transaction transaction = getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新持久化的数据
        transactionConfigurator.getTransactionRepository().update(transaction);

        try {
            // 真正操作提交
            transaction.commit();
            // 删除持久化的数据
            transactionConfigurator.getTransactionRepository().delete(transaction);
        } catch (Throwable commitException) {
            logger.error("compensable transaction confirm failed.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    public Transaction getCurrentTransaction() {
        return threadLocalTransaction.get();
    }

    public void rollback() {
        // 标记事务状态为取消中
        Transaction transaction = getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CANCELLING);
        //更新持久化的数据
        transactionConfigurator.getTransactionRepository().update(transaction);
        
        try {
            // 真正操作回滚
            transaction.rollback();
            // 删除持久化的数据
            transactionConfigurator.getTransactionRepository().delete(transaction);
        } catch (Throwable rollbackException) {
            logger.error("compensable transaction rollback failed.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }
}
