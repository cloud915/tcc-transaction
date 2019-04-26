package org.mengyun.tcctransaction.api;

/**
 * Created by changming.xie on 1/18/17.
 *
 * 传播行为，这个版本最大变化，拆分了事务传播行为；
 * 【TODO 之前版本，类似全部使用的REQUIRES模式，分支事务会创建独立的Transaction对象，但属于嵌套事务，内部事务异常会导致外围事务的回滚；会统一进行commit和rollback]
 * 【TODO 新版本可配置传播行为，更加细化】
 */
public enum Propagation {
    /**
     * 必须的
     * 如果当前没有事务，就新建一个事务，如果已经存在一个事务中，加入到这个事务中。这是最常见的选择。
     */
    REQUIRED(0),
    /**
     * 支持
     * 支持当前事务，如果当前没有事务，就以非事务方式执行。
     */
    SUPPORTS(1),
    /**
     * 强制的、托管的
     * 使用当前的事务，如果当前没有事务，就抛出异常。
     */
    MANDATORY(2),
    /**
     * 新的、必须的
     * 新建事务，如果当前存在事务，把当前事务挂起。
     */
    REQUIRES_NEW(3);

    private final int value;

    private Propagation(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }
}