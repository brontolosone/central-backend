// Copyright 2024 ODK Central Developers
// See the NOTICE file at the top-level directory of this distribution and at
// https://github.com/getodk/central-backend/blob/master/NOTICE.
// This file is part of ODK Central. It is subject to the license terms in
// the LICENSE file found in the top-level directory of this distribution and at
// https://www.apache.org/licenses/LICENSE-2.0. No part of ODK Central,
// including this file, may be copied, modified, propagated, or distributed
// except according to the terms contained in the LICENSE file.

// From earlier form-purging, there could have been leftover client audits
// that were not properly purged because of how they joined to submissions.
// This migration deletes those client audit rows, allowing the blobs to finally
// be de-referenced and also eventually purged (by the puring cron job).

const up = (db) => db.raw(`
BEGIN;

-- Species as citizens. Table for species declarations

CREATE TABLE species (
    -- 36 chars as that's what's in the "actees" table currently, we'll just roll with that, someone thought it reasonable
    name varchar(36) CHECK (name ~ '(\\*|[a-z_]+)') PRIMARY KEY
);


-- Verbs are about a species (as a subject/actee).
-- Though they don't have to be placed on the species that is its subject.
-- For instance, one can place a form.read permission on a project species,
-- and have that project's forms inherit a role's permission to form.read
-- from the project it is part of (via "actees" table).
-- Exclude "open_form.(read|list)" permissions - we'll reformulate those in terms of 
-- a "read_open" and "list_open" verb later on.
WITH theverbs AS (
    SELECT DISTINCT
        (jsonb_array_elements_text(verbs)) AS verb
    FROM
        roles
    ORDER BY
        verb
    )
INSERT INTO species SELECT DISTINCT
    (substring(verb FROM '[a-z]+')) AS species_substring
FROM
    theverbs
WHERE
    verb NOT LIKE 'open_form.%'
ORDER BY
    species_substring;

-- Existing species as mentioned in actees
INSERT INTO species (
    SELECT
        species
    FROM
        actees
    ORDER BY
        species)
ON CONFLICT (name)
    DO NOTHING;

-- Other discovered species
INSERT INTO species VALUES
    ('field_key'),
    ('public_link'),
    ('singleUse'),
    ('actor')
ON CONFLICT (name)
    DO NOTHING;

-- Now we can make the actees table have a proper foreign key to species
ALTER TABLE actees
    ADD CONSTRAINT "actees_species_foreign" FOREIGN KEY (species) REFERENCES species (name)
;


-- Verbs as citizens. Table:
CREATE TABLE authorization_verbs (
    species varchar(36) NOT NULL REFERENCES species (name),
    verbname varchar(36) NOT NULL CHECK (verbname = '*' OR verbname ~ '[a-z_]+'),
    "bitPosition" smallint CHECK ("bitPosition" IS NULL OR "bitPosition" BETWEEN 0 AND 62),
    permission bigint GENERATED ALWAYS AS (
        CASE WHEN "bitPosition" IS NULL
            THEN -((1::bigint << 63)+1) -- maximum positive size of the int64; "all bits (corresponding to positive powers of 2) are set"
            ELSE 1 << "bitPosition"
        END
    ) STORED,
    CHECK (verbname != '*' OR "bitPosition" IS NULL),
    CONSTRAINT "authorization_verbs_primary_key" PRIMARY KEY ("species", "verbname") INCLUDE ("permission"),
    CONSTRAINT "authorization_verbs_permission_scoped_unique" UNIQUE ("species", "verbname", "permission")
);


-- Fill up the verbs table
INSERT INTO authorization_verbs
SELECT
    *
FROM (
    WITH theverbs_split AS (
        WITH theverbs AS (
            SELECT DISTINCT
                (jsonb_array_elements_text(verbs)) AS verb
            FROM
                roles
        )
    SELECT
        substring(verb FROM '[a-z_]*') AS speciespart,
        substring(verb FROM '[^\\.]+\\.(.*)') AS verbnamepart
    FROM
        theverbs
    )
    SELECT
        speciespart,
        verbnamepart,
        (('{
            "create": 0,
            "read": 1,
            "update": 2,
            "delete": 3,
            "list": 4,
            "restore": 5,
            "end": 6,
            "invalidate": 7,
            "reset": 8,
            "run": 9,
            "set": 10
        }'::json) ->> verbnamepart)::integer AS verb_bit
    FROM
        theverbs_split
    WHERE
        speciespart != 'open_form'
    ORDER BY
        speciespart,
        verb_bit,
        verbnamepart ASC
);

-- Add "form.list_open" and "form.read_open", related: https://github.com/getodk/central-backend/pull/968#pullrequestreview-1621236723
INSERT INTO authorization_verbs
    VALUES
    ('form', 'read_open', 6),
    ('form', 'list_open', 7)
;


-- Table for implied authorization verbs
CREATE TABLE authorization_verbs_implied (
    species varchar(36) NOT NULL,
    verbname varchar(36) NOT NULL,
    implied_species varchar(36) NOT NULL,
    implied_verbname varchar(36) NOT NULL,
    CHECK ( (species, verbname) != (implied_species, implied_verbname) ),
    CONSTRAINT authorization_verbs_implied_fk_implicator FOREIGN KEY (species, verbname) REFERENCES authorization_verbs (species, verbname) ON DELETE CASCADE,
    CONSTRAINT authorization_verbs_implied_fk_implicatee FOREIGN KEY (implied_species, implied_verbname) REFERENCES authorization_verbs (species, verbname) ON DELETE CASCADE,
    CONSTRAINT authorization_verbs_implied_primary_key PRIMARY KEY (species, verbname, implied_species, implied_verbname)
);



-- If you can list or read a form (in general), you can list or read it regardless of whether it's open.
-- related: https://github.com/getodk/central-backend/pull/968#pullrequestreview-1621236723
INSERT INTO authorization_verbs_implied
    VALUES
    ('form', 'read', 'form', 'read_open'),
    ('form', 'list', 'form', 'list_open')
;


-- We need a table to store the role-permission associations
CREATE TABLE role_verbs (
    id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    role_id integer NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    species varchar(36) NOT NULL,
    verbname varchar(36) NOT NULL,
    CONSTRAINT role_verbs_fk_authorization_verb FOREIGN KEY (species, verbname) REFERENCES authorization_verbs (species, verbname) ON DELETE CASCADE,
    CONSTRAINT role_verbs_unique_triple UNIQUE (role_id, species, verbname)
);

-- Fill that table with the associations, reaped from the "roles" table...
INSERT INTO role_verbs (role_id, species, verbname)
SELECT
    *
FROM ( WITH theverbs_split AS (
        WITH theverbs AS (
            SELECT
                id AS role_id,
                (jsonb_array_elements_text(verbs)) AS verb
            FROM
                roles
)
            SELECT
                role_id,
                substring(verb FROM '[a-z_]*') AS species,
                substring(verb FROM '[^\\.]+\\.(.*)') AS verbname
            FROM
                theverbs
)
            SELECT
                role_id,
                CASE species
                WHEN 'open_form' THEN
                    'form'
                ELSE
                    species
                END AS species,
                CASE species
                WHEN 'open_form' THEN
                    verbname || '_open'
                ELSE
                    verbname
                END AS verbname
            FROM
                theverbs_split
            ORDER BY
                role_id,
                species,
                verbname
);

-- We'll put a view in place of the table, and so the table needs to be renamed.
ALTER TABLE roles RENAME TO role_definitions;
-- We don't need the "verbs" column anymore; we've store them in the "role_verbs" table now
ALTER TABLE role_definitions DROP COLUMN verbs;


CREATE VIEW "public"."authorization_verbs_implication_closure" AS
    WITH RECURSIVE implication_closure (
        species,
        verbname,
        species_next,
        verbname_next
    ) AS (
        SELECT
            av.species,
            av.verbname,
            avedges1.implied_species,
            avedges1.implied_verbname
        FROM
            authorization_verbs av
            LEFT OUTER JOIN
                authorization_verbs_implied avedges1
                ON (
                    (av.species, av.verbname)
                    = 
                    (avedges1.species, avedges1.verbname)
                )
    UNION ALL
    SELECT
        implication_closure.species,
        implication_closure.verbname,
        avedges2.implied_species,
        avedges2.implied_verbname
    FROM
        implication_closure
        INNER JOIN
            authorization_verbs_implied avedges2
            ON (
                (implication_closure.species_next, implication_closure.verbname_next)
                =
                (avedges2.species, avedges2.verbname)
            )
    )
    SELECT
        ic.species,
        ic.verbname,
        av1.permission,
        ic.species_next as implied_species,
        ic.verbname_next as implied_verbname,
        av2.permission as implied_permission
    FROM
        implication_closure ic
        INNER JOIN
            authorization_verbs av1
            USING (species, verbname)
        LEFT OUTER JOIN
            authorization_verbs av2
            ON (
                (ic.species_next, ic.verbname_next)
                =
                (av2.species, av2.verbname)
            )
    UNION  -- For regularity, add the un-expanded variants in
    SELECT
        species,
        verbname,
        permission,
        NULL,
        NULL,
        NULL
    FROM
        authorization_verbs
;


CREATE VIEW "public"."actor_actee_permissions" AS
        WITH RECURSIVE permission_reduced (
    actor_id,
    actee,
    actee_species,
    parent_actee,
    permission_species,
    permission_bits
) AS (
    WITH direct_permissions AS MATERIALIZED (
        SELECT
            assignments."actorId" AS actor_id,
            actees.id::uuid AS actee,
            actees.species AS actee_species,
            actees.parent::uuid AS parent_actee,
            role_verbs.species AS permission_species,
            bit_or(coalesce(impclosure.implied_permission, impclosure.permission)) AS permission_bits
        FROM
            actees
            INNER JOIN assignments ON (
                (assignments."acteeId" = '*')
                OR
                (actees.id = assignments."acteeId")
            )
            INNER JOIN role_verbs ON (
                (assignments."roleId" = role_verbs.role_id)
            )
            INNER JOIN authorization_verbs_implication_closure impclosure ON (
                (
                    (role_verbs.species, role_verbs.verbname)
                    =
                    (impclosure.species, impclosure.verbname)
                )
                AND 
                (
                    (impclosure.implied_species IS NULL)
                    OR 
                    (impclosure.implied_species = role_verbs.species)
                )
            )
        WHERE (
            actees."purgedAt" IS NULL
            AND actees.id ~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
            AND (
                (actees.parent IS NULL)
                OR
                (actees.parent ~* '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
            )
        )
        GROUP BY (
            actor_id,
            actee,
            actee_species,
            parent_actee,
            permission_species
        )
    )
    SELECT
        *
    FROM
        direct_permissions dp
    WHERE
        dp.parent_actee IS NULL -- startset; roots of inheritance trees
    UNION -- recursive clause; count parents' permissions towards their children
    SELECT
        dp_parent.actor_id,
        dp_child.actee,
        dp_child.actee_species,
        dp_child.parent_actee,
        dp_parent.permission_species,
        dp_parent.permission_bits
    FROM
        direct_permissions dp_parent
        INNER JOIN
        direct_permissions dp_child ON (
            (dp_parent.actee = dp_child.parent_actee)
        )
    UNION
    -- recursive clause; maintain the children's direct permissions as well
    SELECT
        dp_child.actor_id,
        dp_child.actee,
        dp_child.actee_species,
        dp_child.parent_actee,
        dp_child.permission_species,
        dp_child.permission_bits
    FROM
        direct_permissions dp_parent
        INNER JOIN
        direct_permissions dp_child ON (
            (dp_parent.actee = dp_child.parent_actee)
        )
)
SELECT
    actor_id,
    actee,
    actee_species,
    bit_or(permission_bits) AS permission_bits
FROM
    permission_reduced
WHERE
    actee_species = permission_species
GROUP BY (
    actor_id,
    actee,
    actee_species,
    permission_species
);


CREATE VIEW "public"."authorization_verbs_accumulated" AS
        WITH accumulated AS (
            SELECT
                species,
                verbname,
                permission,
                implied_species,
                (
                    CASE WHEN (species = implied_species) THEN
                        permission
                    ELSE
                        0
                    END
                ) | bit_or(implied_permission) AS accumulated_permission
            FROM
                authorization_verbs_implication_closure
            GROUP BY
                species,
                verbname,
                permission,
                implied_species
        )
        SELECT
            species,
            verbname,
            coalesce(implied_species, species) AS implied_species,
            coalesce(accumulated_permission, permission) AS accumulated_permission
        FROM
            accumulated
;



CREATE VIEW "public"."roles" AS
            WITH roleverbs_unique AS (
                WITH roleverbs_implied AS (
                    SELECT
                        rv.role_id,
                        coalesce(imps.implied_species, imps.species) AS species,
                        coalesce(imps.implied_verbname, verbname) AS verbname
                    FROM
                        role_verbs rv
                    NATURAL JOIN authorization_verbs_implication_closure imps
            )
                SELECT
                    *
                FROM
                    roleverbs_implied
                GROUP BY
                    (role_id,
                        species,
                        verbname))
            SELECT
                rd.*,
                json_arrayagg(
                    format('%s.%s', rvu.species, rvu.verbname)
                    ORDER BY (rvu.species, rvu.verbname)
                    RETURNING jsonb
                ) AS verbs
            FROM
                role_definitions rd
                INNER JOIN roleverbs_unique rvu ON (rd.id = rvu.role_id)
            GROUP BY
                rd.id
;


COMMIT;
`);

const down = () => {};

module.exports = { up, down };

