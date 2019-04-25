package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.*;
import org.mengyun.tcctransaction.Terminator;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.MethodType;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 11/8/15.
 */
public class ResourceCoordinatorInterceptor {

    private TransactionConfigurator transactionConfigurator;

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }

    public Object interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        // 找到当前线程的事务信息对象
        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();
        // 如果事务对象不为空（开启了事务：root事务或分支事务）、并且是第一阶段trying
        // 进行参与者处理，将confirm和cancel方法进行封装（其他阶段不需要封装）
        if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRYING)) {

            TransactionContext transactionContext = CompensableMethodUtils.getTransactionContextFromArgs(pjp.getArgs());

            Compensable compensable = getCompensable(pjp);

            MethodType methodType = CompensableMethodUtils.calculateMethodType(transactionContext, compensable != null ? true : false);
            // 根据上下文、注解，分析出方法类型
            switch (methodType) {
                case ROOT:
                    generateAndEnlistRootParticipant(pjp);
                    break;
                case CONSUMER:
                    generateAndEnlistConsumerParticipant(pjp);
                    break;
                case PROVIDER:
                    generateAndEnlistProviderParticipant(pjp);
                    break;
            }
        }
        // 进行切面方法调用
        return pjp.proceed(pjp.getArgs());
    }

    // 反射得到：confirm和cancel方法,封装为InvocationContext==>封装为Terminator==>Participant,加入到事务信息对象中
    private Participant generateAndEnlistRootParticipant(ProceedingJoinPoint pjp) {

        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();
        Compensable compensable = getCompensable(pjp);
        String confirmMethodName = compensable.confirmMethod();
        String cancelMethodName = compensable.cancelMethod();

        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();

        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());

        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());

        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                method.getParameterTypes(), pjp.getArgs());
        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                method.getParameterTypes(), pjp.getArgs());

        Participant participant =
                new Participant(
                        xid,
                        new Terminator(confirmInvocation, cancelInvocation));

        transaction.enlistParticipant(participant);

        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        transactionRepository.update(transaction);

        return participant;
    }

    // 反射得到：切面方法，以它为蓝本封装为confirm和cancel方法的InvocationContext==>Terminator==>Participant,加入到事务信息对象中
    // 封装前，调整方法的TransactionContext参数，状态使用confirming和canceling
    // 【TODO 目前理解，consumer类型的方法，是为了开启子线程并调用provider类型的方法，而子事务必须携带TransactionContext参数】
    // 【TODO 当root事务需要进行conmit或cancel时，在root事务所在线程，只知道consumer方法封装的参与者Participant，分支事务创建了新的Transaction对象】
    // 【TODO 因此，不管commit还是cancel操作，都会导致调用consumer方法，因此封装Participant时，用的是同一个方法，每次调用proxy方法，其身份不同】
    // TODO ？？？为何consumer方法调用时加断点，会导致仓储update异常回滚？？？
    private Participant generateAndEnlistConsumerParticipant(ProceedingJoinPoint pjp) {

        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();

        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();

        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());

        int position = CompensableMethodUtils.getTransactionContextParamPosition(((MethodSignature) pjp.getSignature()).getParameterTypes());

        pjp.getArgs()[position] = new TransactionContext(xid, transaction.getStatus().getId());

        Object[] tryArgs = pjp.getArgs();
        Object[] confirmArgs = new Object[tryArgs.length];
        Object[] cancelArgs = new Object[tryArgs.length];

        System.arraycopy(tryArgs, 0, confirmArgs, 0, tryArgs.length);
        confirmArgs[position] = new TransactionContext(xid, TransactionStatus.CONFIRMING.getId());

        System.arraycopy(tryArgs, 0, cancelArgs, 0, tryArgs.length);
        cancelArgs[position] = new TransactionContext(xid, TransactionStatus.CANCELLING.getId());

        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());

        InvocationContext confirmInvocation = new InvocationContext(targetClass, method.getName(), method.getParameterTypes(), confirmArgs);
        InvocationContext cancelInvocation = new InvocationContext(targetClass, method.getName(), method.getParameterTypes(), cancelArgs);

        Participant participant =
                new Participant(
                        xid,
                        new Terminator(confirmInvocation, cancelInvocation));

        transaction.enlistParticipant(participant);

        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();

        transactionRepository.update(transaction);

        return participant;
    }

    // 反射得到：confirm和cancel方法,封装为InvocationContext==>封装为Terminator==>Participant,加入到事务信息对象中
    private Participant generateAndEnlistProviderParticipant(ProceedingJoinPoint pjp) {

        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();

        Compensable compensable = getCompensable(pjp);

        String confirmMethodName = compensable.confirmMethod();
        String cancelMethodName = compensable.cancelMethod();

        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();

        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());


        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());

        InvocationContext confirmInvocation = new InvocationContext(targetClass, confirmMethodName,
                method.getParameterTypes(), pjp.getArgs());
        InvocationContext cancelInvocation = new InvocationContext(targetClass, cancelMethodName,
                method.getParameterTypes(), pjp.getArgs());

        Participant participant =
                new Participant(
                        xid,
                        new Terminator(confirmInvocation, cancelInvocation));

        transaction.enlistParticipant(participant);

        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        transactionRepository.update(transaction);

        return participant;
    }

    private Compensable getCompensable(ProceedingJoinPoint pjp) {
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();

        Compensable compensable = method.getAnnotation(Compensable.class);

        if (compensable == null) {
            Method targetMethod = null;
            try {
                targetMethod = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());

                if (targetMethod != null) {
                    compensable = targetMethod.getAnnotation(Compensable.class);
                }

            } catch (NoSuchMethodException e) {
                compensable = null;
            }

        }
        return compensable;
    }
}
