package org.mengyun.tcctransaction.unittest.client;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.unittest.service.AccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

/**
 * Created by changmingxie on 12/3/15.
 */
@Service
public class AccountServiceProxy {

    @Autowired
    AccountService accountService;

    private ExecutorService executorService = Executors.newFixedThreadPool(100);

    public void transferFromWithMultipleTier(final TransactionContext transactionContext, final long accountId, final int amount) {
        Future<Boolean> future = this.executorService
                .submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        accountService.transferFromWithMultipleTier(transactionContext, accountId, amount);
                        return true;
                    }
                });

        handleResult(future);
    }

    public void transferToWithMultipleTier(final TransactionContext transactionContext, final long accountId, final int amount) {
        Future<Boolean> future = this.executorService
                .submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        // TODO debug时，这里的断点有时会导致test不通过，猜测是TransactionRecovery流程提前处理了持久化的事务信息，
                        // TODO 正常事务中在update操作时找到不数据抛出异常
                        accountService.transferToWithMultipleTier(transactionContext, accountId, amount);
                        return true;
                    }
                });

        handleResult(future);
    }

    public void performanceTuningTransferTo(TransactionContext transactionContext) {
    }

    public void transferTo(final TransactionContext transactionContext, final long accountId, final int amount) {
        System.out.println("AccountServiceProxy transferTo called,transactionContextStatus="+transactionContext.getStatus());
        Future<Boolean> future = this.executorService
                .submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        accountService.transferTo(transactionContext, accountId, amount);
                        return true;
                    }
                });

        handleResult(future);
    }

    public void transferTo(final long accountId, final int amount) {

        Future<Boolean> future = this.executorService
                .submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        accountService.transferToWithNoTransactionContext(accountId, amount);
                        return true;
                    }
                });

        handleResult(future);
    }

    public void transferFrom(final TransactionContext transactionContext, final long accountId, final int amount) {

        Future<Boolean> future = this.executorService
                .submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        accountService.transferFrom(transactionContext, accountId, amount);
                        return true;
                    }
                });

        handleResult(future);
    }


    private void handleResult(Future<Boolean> future) {
        while (!future.isDone()) {
            //System.out.println("try get future result");
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            future.get();
        } catch (InterruptedException e) {
            throw new Error(e);
        } catch (ExecutionException e) {
            throw new Error(e);
        }
    }


}
