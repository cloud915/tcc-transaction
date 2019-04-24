package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.mengyun.tcctransaction.MethodType;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionConfigurator;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.spring.utils.CompensableMethodUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

/**
 * Created by changmingxie on 10/30/15.
 *
 * 管控事务的处理流程，在何时创建事务信息对象、何时改变事务状态、何时触发事务的不同阶段
 */
@Aspect
public class TccCompensableAspect implements Ordered {

    private int order = Ordered.HIGHEST_PRECEDENCE;

    @Autowired
    private TransactionConfigurator transactionConfigurator;

    @Pointcut("@annotation(org.mengyun.tcctransaction.spring.Compensable)")
    public void compensableService() {

    }

    @Around("compensableService()")
    public void interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {
        // 【分支事务的切面方法，参数列表会包含TransactionContext类型的参数】
        TransactionContext transactionContext = CompensableMethodUtils.getTransactionContextFromArgs(pjp.getArgs());
        // 获取事务信息对象，此时由于未调用TransactionManager的begin()方法，因此还未创建
        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();
        // 根据transactionContext 和是否标记@Compensable 来判断方法类型
        MethodType methodType = CompensableMethodUtils.calculateMethodType(transactionContext, true);

        switch (methodType) {
            case ROOT:
                // 根事务方法
                rootMethodProceed(pjp);
                break;
            case PROVIDER:
                // 提供者方法
                providerMethodProceed(pjp, transactionContext);
                break;
            default:
                // 其他类型直接进行调用，不做处理
                pjp.proceed();
        }
    }
    // 根事务，前置处理
    private void rootMethodProceed(ProceedingJoinPoint pjp) throws Throwable {
        // 开启：定义Transaction 事务信息对象、持久化、缓存
        transactionConfigurator.getTransactionManager().begin();

        try {
            // 进行业务方法调用
            pjp.proceed();
            // 调用正常，进行提交
            transactionConfigurator.getTransactionManager().commit();

        } catch (Throwable commitException) {
            // 调用异常，进行回滚
            try {
                transactionConfigurator.getTransactionManager().rollback();
            } catch (Throwable rollbackException) {
                throw new RuntimeException("compensable transaction rollback failed.", commitException);
            }

            throw commitException;
        }
    }
    // 分支事务，由于方法调用，会重新进入切面
    // 提供者事务，前置处理【分支事务，会包含TransactionContext信息】
    private void providerMethodProceed(ProceedingJoinPoint pjp, TransactionContext transactionContext) throws Throwable {
        // 根据transactionContext中状态=root事务状态，来操作分支事务
        switch (TransactionStatus.valueOf(transactionContext.getStatus())) {
            case TRYING:
                // root事务状态刚启动，需开启分支事务
                transactionConfigurator.getTransactionManager().propagationNewBegin(transactionContext);
                // 进行业务方法调用
                pjp.proceed();
                break;
            case CONFIRMING:
                // root事务进入第二阶段，在commit阶段，如果还有分支事务创建，通过这里保持状态一致
                transactionConfigurator.getTransactionManager().propagationExistBegin(transactionContext);
                // 通过commit()方法，调用@Compensable中定义的commit处理
                transactionConfigurator.getTransactionManager().commit();
                break;
            case CANCELLING:
                // root事务进入第三阶段，在commit阶段，如果还有分支事务创建，通过这里保持状态一致
                transactionConfigurator.getTransactionManager().propagationExistBegin(transactionContext);
                // 通过rollback()方法，调用@Compensable中定义的rollback处理
                transactionConfigurator.getTransactionManager().rollback();
                break;
        }

    }

    @Override
    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }
}
