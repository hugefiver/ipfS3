//! Library-test-only injection AFTER all foundation/schema/binding checks and
//! business writes, immediately before the real durable receipt UPDATE.
use super::*;
use std::cell::RefCell;
#[derive(Clone,Copy)]
pub(crate) enum Fault {ReceiptAbort,ExpireLease}
thread_local! {static NEXT:RefCell<Option<(String,String,Fault)>>=const {RefCell::new(None)};}
pub(crate) fn install(auth:&LocalAuthorization,binding:&ConfirmedBinding,fault:Fault) {
    NEXT.with(|next|*next.borrow_mut()=Some((auth.database_scope.clone(),binding.request.plan_id.clone(),fault)));
}
pub(super) async fn before_write(tx:&DatabaseTransaction,auth:&LocalAuthorization,binding:&ConfirmedBinding,receipt:&ActionReceipt)->Result<()> {
    let fault=NEXT.with(|next|{let mut next=next.borrow_mut();if next.as_ref().is_some_and(|(scope,plan,_)|scope==&auth.database_scope&&plan==&binding.request.plan_id){next.take().map(|(_,_,fault)|fault)}else{None}});
    match fault {
        Some(Fault::ReceiptAbort)=>{
            tx.execute_unprepared("CREATE TRIGGER stage7_actual_receipt_fault BEFORE UPDATE OF result ON pin_management_records WHEN NEW.kind='action' AND NEW.state='applied' BEGIN SELECT RAISE(ABORT,'actual receipt fault'); END").await.map_err(|_|ManagementError::Database)?;
        },
        Some(Fault::ExpireLease)=>{
            let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
            for lease in &receipt.intent_ids {tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_leases SET expires_at=$1 WHERE id=$2",[now.into(),lease.clone().into()])).await.map_err(|_|ManagementError::Database)?;}
        },
        None=>{},
    }Ok(())
}
