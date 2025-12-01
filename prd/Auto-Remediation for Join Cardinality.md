## Auto-Remediation for Join Cardinality

Objectives:

- Report for FSA
- Verifiability

Simplest Implementation:

- Match joins by identifier
  - Prerequisite: ensure all join node identifiers are actually named properly. ~30 minutes to do this
    - Prerequisite I: if there exists a node correspondence, nodes are named identically
    - Prequisite II: If there exists a surplus or net new node in the remediated view, it should not accidentally map to any other join name in the unremediated view
- Produce a report of the join comparisons, exactly the same way we do it for the field cardinalities. The columns are JOIN_NAME, JOIN_TYPE, CARDINALITY. And then a CHECK for those.
- Create a new script that does the remediation. The input is the XLSX
  - Shows a menu that allows you to select which view to use as the input
  - Takes the unremediated view as the source of truth
  - Automatically applies the identical join type and cardinality to the remediated view











