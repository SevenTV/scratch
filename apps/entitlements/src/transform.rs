use std::collections::{HashMap, HashSet};

use crate::binary::{BinaryData, Edge, EdgeKind, Role};

// A set of cosmetic items that should actually be granted to users via roles.
const BADGE_TO_ROLES: &[(&str, &str)] = &[
    ("62f98382e46eb00e438a6971", "6102002eab1aa12bf648cfcd"), // Admin Badge
    ("62f98438e46eb00e438a6972", "60724f65e93d828bf8858789"), // Moderator Badge
    ("62f99bc1e46eb00e438a6981", "60b3f1ea886e63449c5263b1"), // Contributor Badge
    ("62f99c5ae46eb00e438a6982", "608831312a61f51b61f2974d"), // Owner Badge
    ("62f99d0ce46eb00e438a6984", "634494ebddb12c4c4f707a3a"), // Translator Badge
    ("634494ebddb12c4c4f707a3f", "62f99d0ce46eb00e438a6985"), // Event Coordinator Badge
];

fn new_roles() -> Vec<Role> {
    vec![
        Role {
            id: "634494ebddb12c4c4f707a3a".to_string(),
            name: "Translator".to_string(),
            edges: vec![Edge {
                id: "634494ebddb12c4c4f707a3f".to_string(),
                active: true,
                kind: EdgeKind::Badge,
            }],
        },
        Role {
            id: "62f99d0ce46eb00e438a6985".to_string(),
            name: "Event Coordinator".to_string(),
            edges: vec![Edge {
                id: "62f99d0ce46eb00e438a6984".to_string(),
                active: true,
                kind: EdgeKind::Badge,
            }],
        },
    ]
}

#[tracing::instrument(skip(data))]
pub fn transform_data(data: &mut BinaryData) {
    let items = data
        .items
        .drain(..)
        .filter(|cosmetic| !cosmetic.id.is_empty())
        .map(|cosmetic| (cosmetic.id.clone(), cosmetic))
        .collect::<HashMap<_, _>>();

    tracing::info!("transformed {} items", items.len());

    let roles = data
        .roles
        .drain(..)
        .filter(|role| !role.id.is_empty())
        .chain(new_roles().into_iter())
        .map(|mut role| {
            role.edges.retain(|edge| items.contains_key(&edge.id));
            for (cosmetic_id, _) in BADGE_TO_ROLES
                .iter()
                .filter(|(_, role_id)| role_id == &role.id)
            {
                role.edges.push(Edge {
                    id: cosmetic_id.to_string(),
                    kind: EdgeKind::Badge,
                    active: true,
                });
            }

            role.edges.sort();
            role.edges.dedup();

            (role.id.clone(), role)
        })
        .collect::<HashMap<_, _>>();

    tracing::info!("transformed {} roles", roles.len());

    let mut groups = data
        .groups
        .drain(..)
        .filter(|group| !group.id.is_empty())
        .map(|mut group| {
            group.edges.retain(|edge| match edge.kind {
                EdgeKind::Role => roles.contains_key(&edge.id),
                EdgeKind::Badge | EdgeKind::Paint | EdgeKind::EmoteSet => {
                    items.contains_key(&edge.id)
                }
                _ => false,
            });

            (group.id.clone(), group)
        })
        .collect::<HashMap<_, _>>();

    tracing::info!("transformed {} groups", groups.len());

    let products = data
        .products
        .drain(..)
        .filter(|product| !product.id.is_empty())
        .map(|mut product| {
            product.edges.retain(|edge| match edge.kind {
                EdgeKind::Badge | EdgeKind::Paint | EdgeKind::EmoteSet => {
                    items.contains_key(&edge.id)
                }
                EdgeKind::Role => roles.contains_key(&edge.id),
                EdgeKind::Group => {
                    groups.get_mut(&edge.id).map(|group| {
                        group.product_id = product.id.clone();
                    });

                    false
                }
                _ => false,
            });

            product.edges.sort();
            product.edges.dedup();

            (product.id.clone(), product)
        })
        .collect::<HashMap<_, _>>();

    let mut user_products = data
        .user_products
        .drain(..)
        .map(|user_product| (user_product.id.clone(), user_product))
        .collect::<HashMap<_, _>>();

    let user_product_ids = user_products.keys().cloned().collect::<HashSet<_>>();

    user_products.values_mut().for_each(|user_product| {
        user_product.edges.retain_mut(|edge| match edge.kind {
            EdgeKind::Group => groups.contains_key(&edge.id),
            EdgeKind::Product => products.contains_key(&edge.id),
            EdgeKind::UserProduct => user_product_ids.contains(&edge.id),
            _ => false,
        });

        user_product.edges.sort();
        user_product.edges.dedup();
    });

    tracing::info!("transformed {} user products", user_products.len());

    let users = data
        .users
        .drain(..)
        .filter(|user| !user.id.is_empty())
        .map(|mut user| {
            user.edges.retain_mut(|edge| {
                if matches!(edge.kind, EdgeKind::Badge) {
                    if let Some((_, role_id)) = BADGE_TO_ROLES
                        .iter()
                        .find(|(cosmeric, _)| cosmeric == &edge.id)
                    {
                        edge.id = role_id.to_string();
                        edge.kind = EdgeKind::Role;
                    }
                }

                match edge.kind {
                    EdgeKind::Role => roles.contains_key(&edge.id),
                    EdgeKind::Badge | EdgeKind::EmoteSet | EdgeKind::Paint => {
                        items.contains_key(&edge.id)
                    }
                    EdgeKind::Product => products.contains_key(&edge.id),
                    EdgeKind::UserProduct => user_products.contains_key(&edge.id),
                    _ => false,
                }
            });

            user.edges.sort();
            user.edges.dedup();

            (user.id.clone(), user)
        })
        .collect::<HashMap<_, _>>();

    tracing::info!("transformed {} users", users.len());

    data.items = items.into_values().collect();
    data.users = users.into_values().collect();
    data.roles = roles.into_values().collect();
    data.groups = groups.into_values().collect();
    data.products = products.into_values().collect();
    data.user_products = user_products.into_values().collect();
}
