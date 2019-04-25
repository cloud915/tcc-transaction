package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.mengyun.tcctransaction.interceptor.CompensableTransactionInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

/**
 * Created by changmingxie on 10/30/15.
 */
@Aspect
public class TccCompensableAspect implements Ordered {
    // order较小，先执行
    private int order = Ordered.HIGHEST_PRECEDENCE;

    private CompensableTransactionInterceptor compensableTransactionInterceptor;
    // 只针对@Compensable注解的方法
    @Pointcut("@annotation(org.mengyun.tcctransaction.Compensable)")
    public void compensableService() {

    }

    @Around("compensableService()")
    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {
        // 封装核心处理流程
        return compensableTransactionInterceptor.interceptCompensableMethod(pjp);
    }

    @Override
    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public void setCompensableTransactionInterceptor(CompensableTransactionInterceptor compensableTransactionInterceptor) {
        this.compensableTransactionInterceptor = compensableTransactionInterceptor;
    }
}
