htdocs_PROGRAMS = mysqlfs-flow.png
htdocs_DATA = index.html
htdocsdir = $(abs_top_builddir)/htdocs
mysqlfs_flow_png_SOURCES = mysqlfs.dot database.png folder.png page.png processes.png

xdoc: $(foreach n, $(htdocs_PROGRAMS), $(htdocsdir)/$n)
doc: install-htdocsPROGRAMS install-htdocsDATA

if DO_DOT
mysqlfs-flow.png: $(mysqlfs_flow_png_SOURCES)
	$(DOT) -o $@ -Tpng $<
endif
	
