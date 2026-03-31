import falkordb

client = falkordb.FalkorDB(host='localhost', port=6379)
g = client.select_graph('discord_history')

# Step 1: Find all duplicate names
result = g.query("""
  MATCH (n:Entity)
  WITH n.name AS name, collect(n) AS nodes, count(n) AS cnt
  WHERE cnt > 1
  RETURN name, cnt
  ORDER BY cnt DESC
""")

duplicate_names = [row[0] for row in result.result_set]
print(f"Found {len(duplicate_names)} duplicate sets: {duplicate_names}")

merged_count = 0

for name in duplicate_names:
    print(f"\nProcessing: {name!r}")

    # Get all nodes with this name, sorted by created_at ascending
    name_esc = name.replace("'", "\\'")
    nodes_result = g.query(f"""
      MATCH (n:Entity {{name: '{name_esc}'}})
      RETURN n, n.uuid, n.created_at, n.summary
      ORDER BY n.created_at ASC
    """)

    rows = nodes_result.result_set
    if len(rows) < 2:
        print(f"  Only {len(rows)} node(s) found, skipping")
        continue

    print(f"  Found {len(rows)} nodes")

    # The first node (earliest created_at) is the keeper
    keeper_node = rows[0][0]
    keeper_uuid = rows[0][1]
    keeper_created_at = rows[0][2]
    keeper_summary = rows[0][3] or ""

    print(f"  Keeper UUID: {keeper_uuid}, created_at: {keeper_created_at}")

    # Collect summaries from duplicates
    extra_summaries = []
    duplicate_uuids = []
    for row in rows[1:]:
        dup_uuid = row[1]
        dup_summary = row[3] or ""
        duplicate_uuids.append(dup_uuid)
        if dup_summary and dup_summary.strip():
            extra_summaries.append(dup_summary.strip())
        print(f"  Duplicate UUID: {dup_uuid}, created_at: {row[2]}")

    # Build merged summary
    merged_summary = keeper_summary.strip()
    for s in extra_summaries:
        if s not in merged_summary:
            merged_summary = merged_summary + "\n" + s if merged_summary else s

    # Escape single quotes for Cypher
    merged_summary_escaped = merged_summary.replace("\\", "\\\\").replace("'", "\\'")
    keeper_uuid_escaped = keeper_uuid.replace("'", "\\'")

    # Update the keeper's summary
    g.query(f"""
      MATCH (n:Entity {{uuid: '{keeper_uuid_escaped}'}})
      SET n.summary = '{merged_summary_escaped}'
    """)
    print(f"  Updated keeper summary (length: {len(merged_summary)})")

    # For each duplicate, re-point all relationships to/from keeper
    for dup_uuid in duplicate_uuids:
        dup_uuid_escaped = dup_uuid.replace("'", "\\'")

        # 1. Re-point outgoing RELATES_TO from dup to keeper
        rel_result = g.query(f"""
          MATCH (dup:Entity {{uuid: '{dup_uuid_escaped}'}})-[r:RELATES_TO]->(target)
          WHERE target.uuid <> '{keeper_uuid_escaped}'
          RETURN r, target.uuid, r.name, r.fact, r.uuid, r.created_at, r.expired_at, r.valid_at, r.invalid_at, r.episodes
        """)
        for rel_row in rel_result.result_set:
            target_uuid = rel_row[1]
            if not target_uuid:
                continue
            target_uuid_esc = target_uuid.replace("'", "\\'")
            rel_name = (rel_row[2] or "").replace("'", "\\'")
            rel_fact = (rel_row[3] or "").replace("'", "\\'")
            rel_uuid = (rel_row[4] or "").replace("'", "\\'")
            rel_created_at = rel_row[5]
            rel_expired_at = rel_row[6]
            rel_valid_at = rel_row[7]
            rel_invalid_at = rel_row[8]
            rel_episodes = rel_row[9]

            # Check if this relationship already exists from keeper to target
            check = g.query(f"""
              MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}})-[r:RELATES_TO]->(t:Entity {{uuid: '{target_uuid_esc}'}})
              RETURN count(r)
            """)
            existing = check.result_set[0][0] if check.result_set else 0

            if existing == 0:
                props = f"name: '{rel_name}', fact: '{rel_fact}', uuid: '{rel_uuid}'"
                if rel_created_at is not None:
                    props += f", created_at: '{rel_created_at}'"
                if rel_expired_at is not None:
                    props += f", expired_at: '{rel_expired_at}'"
                if rel_valid_at is not None:
                    props += f", valid_at: '{rel_valid_at}'"
                if rel_invalid_at is not None:
                    props += f", invalid_at: '{rel_invalid_at}'"
                g.query(f"""
                  MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}}), (t {{uuid: '{target_uuid_esc}'}})
                  CREATE (k)-[:RELATES_TO {{{props}}}]->(t)
                """)
                print(f"    Created RELATES_TO from keeper to {target_uuid}")

        # 2. Re-point incoming RELATES_TO from sources to keeper
        in_rel_result = g.query(f"""
          MATCH (source)-[r:RELATES_TO]->(dup:Entity {{uuid: '{dup_uuid_escaped}'}})
          WHERE source.uuid <> '{keeper_uuid_escaped}'
          RETURN r, source.uuid, r.name, r.fact, r.uuid, r.created_at, r.expired_at, r.valid_at, r.invalid_at, r.episodes
        """)
        for rel_row in in_rel_result.result_set:
            source_uuid = rel_row[1]
            if not source_uuid:
                continue
            source_uuid_esc = source_uuid.replace("'", "\\'")
            rel_name = (rel_row[2] or "").replace("'", "\\'")
            rel_fact = (rel_row[3] or "").replace("'", "\\'")
            rel_uuid = (rel_row[4] or "").replace("'", "\\'")
            rel_created_at = rel_row[5]
            rel_expired_at = rel_row[6]
            rel_valid_at = rel_row[7]
            rel_invalid_at = rel_row[8]

            check = g.query(f"""
              MATCH (s:Entity {{uuid: '{source_uuid_esc}'}})-[r:RELATES_TO]->(k:Entity {{uuid: '{keeper_uuid_escaped}'}})
              RETURN count(r)
            """)
            existing = check.result_set[0][0] if check.result_set else 0

            if existing == 0:
                props = f"name: '{rel_name}', fact: '{rel_fact}', uuid: '{rel_uuid}'"
                if rel_created_at is not None:
                    props += f", created_at: '{rel_created_at}'"
                if rel_expired_at is not None:
                    props += f", expired_at: '{rel_expired_at}'"
                if rel_valid_at is not None:
                    props += f", valid_at: '{rel_valid_at}'"
                if rel_invalid_at is not None:
                    props += f", invalid_at: '{rel_invalid_at}'"
                g.query(f"""
                  MATCH (s {{uuid: '{source_uuid_esc}'}}), (k:Entity {{uuid: '{keeper_uuid_escaped}'}})
                  CREATE (s)-[:RELATES_TO {{{props}}}]->(k)
                """)
                print(f"    Created RELATES_TO from {source_uuid} to keeper")

        # 3. Re-point outgoing HAS_EPISODE from dup to keeper
        ep_result = g.query(f"""
          MATCH (dup:Entity {{uuid: '{dup_uuid_escaped}'}})-[r:HAS_EPISODE]->(ep)
          RETURN ep.uuid
        """)
        for ep_row in ep_result.result_set:
            ep_uuid = ep_row[0]
            if not ep_uuid:
                continue
            ep_uuid_esc = ep_uuid.replace("'", "\\'")
            check = g.query(f"""
              MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}})-[:HAS_EPISODE]->(ep {{uuid: '{ep_uuid_esc}'}})
              RETURN count(*)
            """)
            existing = check.result_set[0][0] if check.result_set else 0
            if existing == 0:
                g.query(f"""
                  MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}}), (ep {{uuid: '{ep_uuid_esc}'}})
                  CREATE (k)-[:HAS_EPISODE]->(ep)
                """)
                print(f"    Created HAS_EPISODE from keeper to episode {ep_uuid}")

        # 4. Re-point NEXT_EPISODE from/to dup
        next_out = g.query(f"""
          MATCH (dup:Entity {{uuid: '{dup_uuid_escaped}'}})-[r:NEXT_EPISODE]->(nep)
          RETURN nep.uuid
        """)
        for ep_row in next_out.result_set:
            nep_uuid = ep_row[0]
            if not nep_uuid:
                continue
            nep_uuid_esc = nep_uuid.replace("'", "\\'")
            check = g.query(f"""
              MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}})-[:NEXT_EPISODE]->(nep {{uuid: '{nep_uuid_esc}'}})
              RETURN count(*)
            """)
            existing = check.result_set[0][0] if check.result_set else 0
            if existing == 0:
                g.query(f"""
                  MATCH (k:Entity {{uuid: '{keeper_uuid_escaped}'}}), (nep {{uuid: '{nep_uuid_esc}'}})
                  CREATE (k)-[:NEXT_EPISODE]->(nep)
                """)
                print(f"    Created NEXT_EPISODE from keeper to {nep_uuid}")

        next_in = g.query(f"""
          MATCH (prev)-[r:NEXT_EPISODE]->(dup:Entity {{uuid: '{dup_uuid_escaped}'}})
          RETURN prev.uuid
        """)
        for ep_row in next_in.result_set:
            prev_uuid = ep_row[0]
            if not prev_uuid:
                continue
            prev_uuid_esc = prev_uuid.replace("'", "\\'")
            check = g.query(f"""
              MATCH (prev {{uuid: '{prev_uuid_esc}'}})-[:NEXT_EPISODE]->(k:Entity {{uuid: '{keeper_uuid_escaped}'}})
              RETURN count(*)
            """)
            existing = check.result_set[0][0] if check.result_set else 0
            if existing == 0:
                g.query(f"""
                  MATCH (prev {{uuid: '{prev_uuid_esc}'}}), (k:Entity {{uuid: '{keeper_uuid_escaped}'}})
                  CREATE (prev)-[:NEXT_EPISODE]->(k)
                """)
                print(f"    Created NEXT_EPISODE from {prev_uuid} to keeper")

        # 5. Delete all relationships on the duplicate node, then delete the node
        g.query(f"""
          MATCH (dup:Entity {{uuid: '{dup_uuid_escaped}'}})
          DETACH DELETE dup
        """)
        print(f"  Deleted duplicate node {dup_uuid}")

    merged_count += 1
    print(f"  Done merging {name!r}")

print(f"\n=== Merged {merged_count} duplicate node sets ===")

# Verify
verify = g.query("""
  MATCH (n:Entity)
  WITH n.name AS name, count(n) AS cnt
  WHERE cnt > 1
  RETURN name, cnt
""")
if verify.result_set:
    print(f"WARNING: Still {len(verify.result_set)} duplicate sets remaining:")
    for row in verify.result_set:
        print(f"  {row[0]}: {row[1]}")
else:
    print("Verification passed: no duplicate Entity nodes remain.")
