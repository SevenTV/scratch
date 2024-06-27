## Entitlements System

- assign roles, badges, emote sets, paints directly to a user
- assign badges, paints, emote sets to a role
- users can purchase products
- products can give roles, badges, paints, emote sets
- products can also be subscriptions
- subscriptions can give roles, badges, paints, emote sets or other products / subscriptions
- subscriptions can have conditional entitlements requirements, they must be subbed for a certain amount of time to get this entitlement or they must have been subscribed during a certain time period.
- Additionally users can be given entitlements if they perform some action, such as gifting x amount of items during a certain time period.


- need be able to change the entitlements system without changing the code
- need be able to update a role, user, product or subscription's entitlements and have them back populated to the user


- what is a condition?:
    - subscription:
        duration and or time period
    - gift:
        amount and or time period

## Emote Set System

- emotes can be assigned to an emote set(s)
- emote set(s) can be active on a user(s)

- need to be able to count the number of unique users that have an emote active
- need to be able to get a list of all the users that have a specific emote/emote-set active
- need to be able to get a list of all the emotes/emote-sets that a user has active

## Examples

```yaml
SubscriptionA:
    - role: RoleA
    - badge: BadgeA
    - paint: PaintA
    - emote_set: EmoteSetA
    - entitlements:
        - condition: duration
          duration: 1 month
          badge: BadgeB
        - condition: duration
          duration: 3 months
          badge: BadgeC
```