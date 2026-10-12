-- 网关池账本不再保留上一身份的归档：换成员即从空账本开始。
-- 清掉存量的 *_previous_tag 键与嵌套 previous；只动对象值（#- 遇数组会报错），重放无副作用。
UPDATE accounts
SET extra = extra - ARRAY['openai_gwpool_previous_ledger_tag', 'openai_gwpool_usage_previous_tag', 'openai_gwpool_rest_previous_tag']
WHERE extra ?| ARRAY['openai_gwpool_previous_ledger_tag', 'openai_gwpool_usage_previous_tag', 'openai_gwpool_rest_previous_tag'];

UPDATE accounts SET extra = extra #- '{openai_gwpool_usage_rounds,previous}'
WHERE jsonb_typeof(extra->'openai_gwpool_usage_rounds') = 'object' AND extra->'openai_gwpool_usage_rounds' ? 'previous';

UPDATE accounts SET extra = extra #- '{openai_gwpool_contacts,previous}'
WHERE jsonb_typeof(extra->'openai_gwpool_contacts') = 'object' AND extra->'openai_gwpool_contacts' ? 'previous';

UPDATE accounts SET extra = extra #- '{openai_gwpool_gateways,previous}'
WHERE jsonb_typeof(extra->'openai_gwpool_gateways') = 'object' AND extra->'openai_gwpool_gateways' ? 'previous';

UPDATE accounts SET extra = extra #- '{openai_gwpool_rest_state,previous}'
WHERE jsonb_typeof(extra->'openai_gwpool_rest_state') = 'object' AND extra->'openai_gwpool_rest_state' ? 'previous';
