package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.*;
import org.mengyun.tcctransaction.Terminator;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.spring.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.spring.utils.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 11/8/15.
 *
 * 管理参与者，何时创建参与者，如何创建参与者，参与者的参数赋值
 * 参与者就是切面方法的confirm和cancel方法
 */
@Aspect
public class TccTransactionContextAspect implements Ordered {

    private int order = Ordered.HIGHEST_PRECEDENCE + 1;

    @Autowired
    private TransactionConfigurator transactionConfigurator;

    @Pointcut("execution(public * *(org.mengyun.tcctransaction.api.TransactionContext,..))||@annotation(org.mengyun.tcctransaction.spring.Compensable)")
    public void transactionContextCall() {

    }

    @Around("transactionContextCall()")
    public void interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        // 如果TccCompensableAspect中开启了事务，此处会有transaction
        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();
        // 只有第一阶段流程，才会进入参与者处理【root事务、分支事务的第一阶段】
        if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRYING)) {
            // 如果是事务的第一阶段，从参数列中拿到TransactionContext
            TransactionContext transactionContext = CompensableMethodUtils.getTransactionContextFromArgs(pjp.getArgs());
            // 找到切面方法的注解对象
            Compensable compensable = getCompensable(pjp);
            // 根据TransactionContext和Compensable判断是哪种方法类型
            MethodType methodType = CompensableMethodUtils.calculateMethodType(transactionContext, compensable != null ? true : false);

            switch (methodType) {
                case ROOT:
                    // root事务方法处理
                    generateAndEnlistRootParticipant(pjp);
                    break;
                case CONSUMER:
                    // 消费者方法处理【第二、第三阶段方法，以及中间其他的方法】
                    generateAndEnlistConsumerParticipant(pjp);
                    break;
                case PROVIDER:
                    // 提供者方法处理
                    generateAndEnlistProviderParticipant(pjp);
                    break;
            }
        }
        // 普通方法，不做事务控制
        pjp.proceed(pjp.getArgs());
    }

    // root事务的Participant参与者处理
    private Participant generateAndEnlistRootParticipant(ProceedingJoinPoint pjp) {
        // 切面方法签名
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        // 切面方法
        Method method = signature.getMethod();
        // 找到切面方法的注解对象
        Compensable compensable = getCompensable(pjp);
        // 找到第二、三阶段方法名
        String confirmMethodName = compensable.confirmMethod();
        String cancelMethodName = compensable.cancelMethod();
        // 由于是root事务，在TccCompensableAspect切面中，已经创建了Transaction
        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();
        // 拿到全局id
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());
        // 通过反射，找到 调用类
        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());
        // 生成confirm上下文【包含了targetClass中所有信息，足够后面对confirm方法的调用】
        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                method.getParameterTypes(), pjp.getArgs());
        // 生成cancel上下文【包含了targetClass中所有信息，足够后面对cancel方法的调用】
        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                method.getParameterTypes(), pjp.getArgs());
        // 生成参与者，包含 confirm和cancel的上下文信息
        Participant participant =
                new Participant(
                        xid,
                        new Terminator(confirmInvocation, cancelInvocation));
        // 记录到Transaction事务信息对象中
        transaction.enlistParticipant(participant);
        // 持久化、缓存
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        transactionRepository.update(transaction);

        return participant;
    }
    // 消费者方法的参与者处理【第二、第三阶段，以及中间其他的方法】
    //org.mengyun.tcctransaction.unittest.client.AccountServiceProxy.transferTo(org.mengyun.tcctransaction.api.TransactionContext, long, int)
    private Participant generateAndEnlistConsumerParticipant(ProceedingJoinPoint pjp) {
        // 拿到切面方法
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();
        // 拿到事务信息对象
        Transaction transaction = transactionConfigurator.getTransactionManager().getCurrentTransaction();
        // 获取全局事务id
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());
        // 找到参数列表中，TransactionContext类型参数的索引/位置
        int position = CompensableMethodUtils.getTransactionContextParamPosition(((MethodSignature) pjp.getSignature()).getParameterTypes());
        // 【由于消费者方法的TransactionContext为空】创建一个TransactionContext，状态与root事务状态一致
        pjp.getArgs()[position] = new TransactionContext(xid, transaction.getStatus().getId());

        // 将切面方法的参数列表，赋值给confirm和cancel两个方法
        Object[] tryArgs = pjp.getArgs();
        Object[] confirmArgs = new Object[tryArgs.length];
        Object[] cancelArgs = new Object[tryArgs.length];

        System.arraycopy(tryArgs, 0, confirmArgs, 0, tryArgs.length);
        // 调整 confirm方法的上下文事务状态
        confirmArgs[position] = new TransactionContext(xid, TransactionStatus.CONFIRMING.getId());

        System.arraycopy(tryArgs, 0, cancelArgs, 0, tryArgs.length);
        // 调整 confirm方法的上下文事务状态
        cancelArgs[position] = new TransactionContext(xid, TransactionStatus.CANCELLING.getId());

        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());
        // 用当前方法的参数信息生成confirm和cancel的上下文【TODO 这里有问题，产生死循环调用】
        InvocationContext confirmInvocation = new InvocationContext(targetClass, method.getName(), method.getParameterTypes(), confirmArgs);
        InvocationContext cancelInvocation = new InvocationContext(targetClass, method.getName(), method.getParameterTypes(), cancelArgs);
        // 创建参与者对象
        Participant participant =
                new Participant(
                        xid,
                        new Terminator(confirmInvocation, cancelInvocation));
        // 加入事务信息对象
        transaction.enlistParticipant(participant);
        // 持久化、缓存
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        transactionRepository.update(transaction);

        return participant;
    }
    // 分支事务的参与者处理，与root事务一致
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

    @Override
    public int getOrder() {
        return -10;
    }

    public void setOrder(int order) {
        this.order = order;
    }
}
