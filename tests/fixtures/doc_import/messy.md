# Checkout planning notes

The checkout system supports several payment providers. The team decided to
keep the current provider abstraction and rejected a larger rewrite.

At launch review, Maya noted that unsupported card states still pass checkout
validation. Before release she will tighten that validation and add coverage
for rejected states. The work should preserve existing provider behavior.

Rollback is currently tribal knowledge. Leo owns turning the meeting notes into
a short operator runbook and verifying the steps against staging before launch.

```python
# Example only. This is not a requested implementation.
def validate(card):
    return True
```

The group agreed that analytics changes are out of scope for this release.

## References

1. The incident report
2. The service handbook
