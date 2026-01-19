-- Copyright 2026 ODK Central Developers
-- See the NOTICE file at the top-level directory of this distribution and at
-- https://github.com/getodk/central-backend/blob/master/NOTICE.
-- This file is part of ODK Central. It is subject to the license terms in
-- the LICENSE file found in the top-level directory of this distribution and at
-- https://www.apache.org/licenses/LICENSE-2.0. No part of ODK Central,
-- including this file, may be copied, modified, propagated, or distributed
-- except according to the terms contained in the LICENSE file.

--- create: hash_aggregate_state_transition_func(state text, additional_value anyelement) ---
CREATE FUNCTION "public"."hash_aggregate_state_transition_func"(state text, additional_value anyelement)
RETURNS text
AS
    $BODY$
        SELECT md5(
            coalesce(state, 'NULL')
            || ' '
            || coalesce(
                md5(additional_value::text),
                'NULL'
            )
        )
    $BODY$
LANGUAGE sql
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
;

CREATE AGGREGATE md5_hash_agg(anyelement) (
    SFUNC    = hash_aggregate_state_transition_func,
    STYPE    = text,
    INITCOND = NULL
);

--- sign: hash_aggregate_state_transition_func(state text, additional_value anyelement) ---
COMMENT ON FUNCTION "public"."hash_aggregate_state_transition_func"(state text, additional_value anyelement) IS '{"dbsamizdat": {"version": 1, "definition_hash": "6e42c0f6ad99ec4fe0eb9dd20526994a"}}';
