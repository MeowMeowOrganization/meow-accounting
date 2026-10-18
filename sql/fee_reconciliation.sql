-- Both anomaly queries must return ZERO ROWS. Do not "correct" financial
-- history by editing posted balances or ledger entries.

-- 1. Trade ledger has fee-position entries but no immutable fee receipt,
--    or an unsupported fee ledger entry position.
SELECT lt.ledger_transaction_id, lt.reference_id AS trade_id,
       le.entry_index
FROM accounting.ledger_transactions AS lt
JOIN accounting.ledger_entries AS le USING (ledger_transaction_id)
LEFT JOIN accounting.trade_fees AS f ON f.trade_id = lt.reference_id
WHERE lt.reference_type = 'TRADE'
  AND le.entry_index >= 4
  AND (f.trade_id IS NULL OR le.entry_index >= 8);

-- 2. Fee receipt disagrees with the immutable signed posting; also flags
--    missing entry pairs and unexpected entry pairs for zero-fee sides.
SELECT f.trade_id, f.collector_account_id, f.quote_asset,
       f.buyer_fee_atoms, f.seller_fee_atoms
FROM accounting.trade_fees AS f
JOIN accounting.trade_settlements AS s USING (trade_id)
JOIN accounting.ledger_transactions AS lt ON lt.operation_id = s.operation_id
LEFT JOIN accounting.ledger_entries AS principal_buyer
  ON principal_buyer.ledger_transaction_id = lt.ledger_transaction_id
 AND principal_buyer.entry_index = 2
LEFT JOIN accounting.ledger_entries AS principal_seller
  ON principal_seller.ledger_transaction_id = lt.ledger_transaction_id
 AND principal_seller.entry_index = 3
LEFT JOIN accounting.ledger_entries AS buyer_debit
  ON buyer_debit.ledger_transaction_id = lt.ledger_transaction_id
 AND buyer_debit.entry_index = 4
LEFT JOIN accounting.ledger_entries AS buyer_credit
  ON buyer_credit.ledger_transaction_id = lt.ledger_transaction_id
 AND buyer_credit.entry_index = 5
LEFT JOIN accounting.ledger_entries AS seller_debit
  ON seller_debit.ledger_transaction_id = lt.ledger_transaction_id
 AND seller_debit.entry_index = 6
LEFT JOIN accounting.ledger_entries AS seller_credit
  ON seller_credit.ledger_transaction_id = lt.ledger_transaction_id
 AND seller_credit.entry_index = 7
WHERE lt.reference_type IS DISTINCT FROM 'TRADE'
   OR lt.reference_id IS DISTINCT FROM f.trade_id
   OR lt.state IS DISTINCT FROM 'POSTED'
   OR principal_buyer.asset_code IS DISTINCT FROM f.quote_asset
   OR principal_seller.asset_code IS DISTINCT FROM f.quote_asset
   OR (
       f.buyer_fee_atoms > 0 AND (
           buyer_debit.account_id IS DISTINCT FROM principal_buyer.account_id
        OR buyer_debit.asset_code IS DISTINCT FROM f.quote_asset
        OR buyer_debit.amount_atoms IS DISTINCT FROM -f.buyer_fee_atoms
        OR buyer_credit.account_id IS DISTINCT FROM f.collector_account_id
        OR buyer_credit.asset_code IS DISTINCT FROM f.quote_asset
        OR buyer_credit.amount_atoms IS DISTINCT FROM f.buyer_fee_atoms
       )
   )
   OR (
       f.buyer_fee_atoms = 0 AND
       (buyer_debit.entry_index IS NOT NULL OR buyer_credit.entry_index IS NOT NULL)
   )
   OR (
       f.seller_fee_atoms > 0 AND (
           seller_debit.account_id IS DISTINCT FROM principal_seller.account_id
        OR seller_debit.asset_code IS DISTINCT FROM f.quote_asset
        OR seller_debit.amount_atoms IS DISTINCT FROM -f.seller_fee_atoms
        OR seller_credit.account_id IS DISTINCT FROM f.collector_account_id
        OR seller_credit.asset_code IS DISTINCT FROM f.quote_asset
        OR seller_credit.amount_atoms IS DISTINCT FROM f.seller_fee_atoms
       )
   )
   OR (
       f.seller_fee_atoms = 0 AND
       (seller_debit.entry_index IS NOT NULL OR seller_credit.entry_index IS NOT NULL)
   );

-- Reporting (NOT anomaly): fee totals by quote asset and fee collector.
SELECT collector_account_id, quote_asset,
       SUM(buyer_fee_atoms) AS buyer_fee_atoms,
       SUM(seller_fee_atoms) AS seller_fee_atoms,
       SUM(buyer_fee_atoms + seller_fee_atoms) AS total_fee_atoms
FROM accounting.trade_fees
GROUP BY collector_account_id, quote_asset
ORDER BY collector_account_id, quote_asset;
