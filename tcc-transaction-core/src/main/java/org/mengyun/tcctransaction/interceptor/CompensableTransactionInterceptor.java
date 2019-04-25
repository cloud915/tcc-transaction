package org.mengyun.tcctransaction.interceptor;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.NoExistedTransactionException;
import org.mengyun.tcctransaction.OptimisticLockException;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.MethodType;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 10/30/15.
 */
public class CompensableTransactionInterceptor {

    static final Logger logger = Logger.getLogger(CompensableTransactionInterceptor.class.getSimpleName());


    private TransactionConfigurator transactionConfigurator;


    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }

    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {

        // 从参数列表中获取事务上下文
        TransactionContext transactionContext = CompensableMethodUtils.getTransactionContextFromArgs(pjp.getArgs());
        // 判断被调用方法类型
        MethodType methodType = CompensableMethodUtils.calculateMethodType(transactionContext, true);

        switch (methodType) {
            case ROOT:
                // root事务
                return rootMethodProceed(pjp);
            case PROVIDER:
                // 分支事务
                return providerMethodProceed(pjp, transactionContext);
            default:
                // 普通方法，取消了消费者方法的处理
                return pjp.proceed();
        }
    }

    private Object rootMethodProceed(ProceedingJoinPoint pjp) throws Throwable {
        // 开启事务：持久化+缓存 事务信息对象
        transactionConfigurator.getTransactionManager().begin();

        Object returnValue = null;
        try {
            returnValue = pjp.proceed();
        } catch (OptimisticLockException e) {
            //内部定义的异常，不回滚，交给补偿逻辑
            throw e; //do not rollback, waiting for recovery job
        } catch (Throwable tryingException) {
            // 其他异常，回滚事务
            logger.warn("compensable transaction trying failed.", tryingException);
            transactionConfigurator.getTransactionManager().rollback();
            // 中断执行流程
            throw tryingException;
        }
        // 未发生异常，提交事务
        transactionConfigurator.getTransactionManager().commit();

        return returnValue;
    }
    // TODO 当进入commit或rollback阶段，provider方法通过proxy调用时，切面生效
    // TODO 此时，switch发挥作用，根据上下文中的状态，判断处于哪个阶段，来进行不同操作，只有trying阶段才会执行切面方法
    private Object providerMethodProceed(ProceedingJoinPoint pjp, TransactionContext transactionContext) throws Throwable {
        // 判断当前事务状态，分支事务的上下文肯定不为空
        // 上下文中的状态，是在ResourceCoordinatorInterceptor流程中赋值的，标记的是root事务状态
        switch (TransactionStatus.valueOf(transactionContext.getStatus())) {
            case TRYING:
                // root事务为trying节点，分支事务开启、初始化信息保持与root事务一致
                transactionConfigurator.getTransactionManager().propagationNewBegin(transactionContext);
                // 执行切面方法
                return pjp.proceed();
            case CONFIRMING:
                try {
                    // 进入commit阶段，分支事务要保持一致状态
                    transactionConfigurator.getTransactionManager().propagationExistBegin(transactionContext);
                    // 进行commit操作，从之前注册的confim列表中，逐个提交
                    transactionConfigurator.getTransactionManager().commit();
                } catch (NoExistedTransactionException excepton) {
                    //the transaction has been commit,ignore it.
                }
                break;
            case CANCELLING:
                // 同commit操作流程，进行rollback操作
                try {
                    transactionConfigurator.getTransactionManager().propagationExistBegin(transactionContext);
                    transactionConfigurator.getTransactionManager().rollback();
                } catch (NoExistedTransactionException exception) {
                    //the transaction has been rollback,ignore it.
                }
                break;
        }
        // 非预置状态处理：找到切面方法返回值类型，以空值返回
        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();

        return ReflectionUtils.getNullValue(method.getReturnType());
    }

}
