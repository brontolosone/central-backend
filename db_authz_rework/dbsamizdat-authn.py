from dbsamizdat import SamizdatView

class actor_actee_permissions(SamizdatView):
    deps_on_unmanaged = {
        'actees',
        'assignments',
        'role_verbs',
        'authorization_verbs_implication_closure',
    }
    sql_template = """
        ${preamble}
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
        )
    """