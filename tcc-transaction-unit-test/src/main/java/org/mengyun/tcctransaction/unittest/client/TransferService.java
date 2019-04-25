package org.mengyun.tcctransaction.unittest.client;

import org.mengyun.tcctransaction.Compensable;
import org.mengyun.tcctransaction.unittest.entity.AccountStatus;
import org.mengyun.tcctransaction.unittest.entity.SubAccount;
import org.mengyun.tcctransaction.unittest.repository.SubAccountRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by changmingxie on 10/25/15.
 */
@Service
public class TransferService {

    @Autowired
    AccountServiceProxy accountService;

    @Autowired
    SubAccountRepository subAccountRepository;

    public TransferService() {
    }


    @Compensable
    @Transactional
    public void performenceTuningTransfer() {
        accountService.performanceTuningTransferTo(null);
    }

    @Compensable(confirmMethod = "transferConfirm", cancelMethod = "transferCancel")
    @Transactional
    public void transfer(long fromAccountId, long toAccountId, int amount) {

        System.out.println("[TransferService] transfer called");

        SubAccount subAccount = subAccountRepository.findById(fromAccountId);

        subAccount.setStatus(AccountStatus.TRANSFERING.getId());

        subAccount.setBalanceAmount(subAccount.getBalanceAmount() - amount);

        accountService.transferTo(null, toAccountId, amount);
        System.out.println("[TransferService] transfer called end");
    }

    // 串行多节点调用，分支事务逐级向下开启
    @Compensable(confirmMethod = "transferConfirm", cancelMethod = "transferCancel")
    public void transferWithMultipleTier(long fromAccountId, long toAccountId, int amount) {

        System.out.println("transferWithMultipleTier called");

        SubAccount subAccount = subAccountRepository.findById(fromAccountId);

        subAccount.setStatus(AccountStatus.TRANSFERING.getId());

        subAccount.setBalanceAmount(subAccount.getBalanceAmount() - amount);

        accountService.transferToWithMultipleTier(null, toAccountId, amount);
    }
    // 并行多节点调用，分支事务同级别
    @Compensable(confirmMethod = "transferWithMultipleConsumerConfirm", cancelMethod = "transferWithMultipleConsumerCancel")
    @Transactional
    public void transferWithMultipleConsumer(long fromAccountId, long toAccountId, int amount) {
        System.out.println("transferWithMultipleConsumer called");
        accountService.transferFrom(null, fromAccountId, amount);
        accountService.transferTo(null, toAccountId, amount);
    }
    @Compensable
    public void transferWithOnlyTryAndMultipleConsumer(long fromAccountId, long toAccountId, int amount) {
        System.out.println("transferWithOnlyTryAndMultipleConsumer called");
        accountService.transferFrom(null, fromAccountId, amount);
        accountService.transferTo(null, toAccountId, amount);
    }

    @Compensable
    public void transferWithNoTransactionContext(long fromAccountId, long toAccountId, int amount) {
        System.out.println("transferWithNoTransactionContext called");
        accountService.transferTo(toAccountId, amount);
    }

    public void transferConfirm(long fromAccountId, long toAccountId, int amount) {
        System.out.println("TransferService transferConfirm called");
        SubAccount subAccount = subAccountRepository.findById(fromAccountId);
        subAccount.setStatus(AccountStatus.NORMAL.getId());
        System.out.println("TransferService transferConfirm called end");
    }

    public void transferCancel(long fromAccountId, long toAccountId, int amount) {

        System.out.println("[TransferService] transferCancel called");

        SubAccount subAccount = subAccountRepository.findById(fromAccountId);

        if (subAccount.getStatus() == AccountStatus.TRANSFERING.getId()) {
            subAccount.setBalanceAmount(subAccount.getBalanceAmount() + amount);
        }

        subAccount.setStatus(AccountStatus.NORMAL.getId());
        System.out.println("[TransferService] transferCancel called end");
    }

    public void transferWithMultipleConsumerConfirm(long fromAccountId, long toAccountId, int amount) {
        System.out.println("transferWithMultipleConsumerConfirm called");
    }

    public void transferWithMultipleConsumerCancel(long fromAccountId, long toAccountId, int amount) {
        System.out.println("transferWithMultipleConsumerCancel called");
    }
}
