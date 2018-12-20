library(VennDiagram)
grid.newpage()
draw.quad.venn(area1=1816, area2=1529, area3=1628, area4=1501, n12=1379, n13=1336, n14=1197, n23=1310, n24=1182, n34=1326, n123=1234, n124=1122, n134=1155, n234=1149, n1234=1101, category=c("2014","2015","2016","2017"), fill=c("red","blue", "orange","green"))
