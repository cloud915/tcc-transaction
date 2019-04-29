package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;

/**
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());
    // 去掉TransactionConfigurator，直接使用TransactionRepository
    private TransactionRepository transactionRepository;
    // 由队ThreadLocal<Deque<Transaction>> 代替ThreadLocal<Transaction>；作用一样
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void begin() {
        // 开启事务，持久化、缓存
        Transaction transaction = new Transaction(TransactionType.ROOT);
        transactionRepository.create(transaction);
        // 入队，且
        registerTransaction(transaction);
    }

    public void propagationNewBegin(TransactionContext transactionContext) {
        // 开启分支事务，携带上下文，持久化、缓存
        Transaction transaction = new Transaction(transactionContext);
        transactionRepository.create(transaction);

        registerTransaction(transaction);
    }

    public void propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        // 分支事务状态变更
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            // 入队，propagationExistBegin执行完成后，后面会带有出队操作
            registerTransaction(transaction);
        } else {
            throw new NoExistedTransactionException();
        }
    }

    public void commit() {
        // 提交事务：变革事务状态、执行commit、删除事务信息的持久化、缓存
        Transaction transaction = getCurrentTransaction();

        transaction.changeStatus(TransactionStatus.CONFIRMING);

        transactionRepository.update(transaction);

        try {
            transaction.commit();
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            logger.error("compensable transaction confirm failed.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();
        }
        return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    public void rollback() {
        // 提交事务：变革事务状态、执行rollback、删除事务信息的持久化、缓存
        Transaction transaction = getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CANCELLING);

        transactionRepository.update(transaction);

        try {
            transaction.rollback();
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            logger.error("compensable transaction rollback failed.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }
    // 入队，方法调用与出队成对出现
    private void registerTransaction(Transaction transaction) {

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);
    }
    // 出队，方法调用与入队成对出现
    public void cleanAfterCompletion() {
        CURRENT.get().pop();
    }

    // 注册参与者
    public void enlistParticipant(Participant participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.enlistParticipant(participant);
        transactionRepository.update(transaction);
    }
}
