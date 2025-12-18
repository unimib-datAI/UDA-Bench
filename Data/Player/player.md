# Join key match

During labeling, we observed that the same real-world entity is often referred to by different surface forms across documents. Therefore, we perform entity matching (canonicalization) during joins and aggregations. The following example illustrates this issue.

## manager.name = team.ownership
manager.name=Steven Anthony Ballmer <-> team.ownership=Steve Ballmer
manager.name=Tilman Joseph Fertitta <-> team.ownership=Tilman Fertitta
manager.name=Joseph Chung-Hsin Tsai <-> team.ownership=Joseph Tsai
manager.name=Herbert Simon <-> team.ownership=Herb Simon
manager.name=Jerry Michael Reinsdorf <-> team.ownership=Jerry Reinsdorf
manager.name=Gayle Marie LaJaunie Bird Benson <-> team.ownership=Gayle Benson
manager.name=Enos Stanley Kroenke <-> team.ownership=Stan Kroenke
manager.name=James Lawrence Dolan <-> team.ownership=James L. Dolan
manager.name=Joseph Steven Lacob <-> team.ownership=Joe Lacob
