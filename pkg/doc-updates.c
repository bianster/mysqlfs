/**
 * @page updates Updates History
 *
 * Don't worry about this, it might disappear in the future.  I'm tracking some changes into the
 * revisions until 0.4.0 is released.  I'll probably roll-up tha thcnages then, and start the next
 * bump, optimistic that we'll have that herd of changes.
 * 
 * @dot
   digraph stack {
      node [shape=record, fontname=Helvetica, fontsize=10];
      "0.3.1" [ label="0.3.1\n2006-10-03" ];
      "0.3.99.0" [ label="0.4.0-rc1\n0.3.99.0\n2007-03-29" ];
      "0.3.99.1" [ label="0.3.99.1\n2007-03-29" ];
      "0.3.1" -> "0.3.99.0" -> "0.3.99.1";
      "0.3.99.2" [ label="0.3.99.2\n2009-07-15" ];

      node [ color=lightblue, shape=ellipse, fontname=Helvetica, fontsize=10, URL="https://sourceforge.net/tracker/?func=detail&group_id=129981&atid=716425&aid=\N" ];
      "1841631" [ label="\N\n(Compression)" ]; 
      "0.3.99.1" -> "1841631";
      "1773343" [ label="\N\n(usage)" ];
      "0.3.99.1" -> "1773343";
      "1773343" -> "0.3.99.2" [headlabel="r50-51"];

      node [ color=lightblue, shape=ellipse, fontname=Helvetica, fontsize=10];
      "specfile" -> "0.3.99.0" [headlabel="r46"];
      "macosx" -> "0.3.99.0" [headlabel="r43"];
      "doxygen" -> "0.3.99.1" [headlabel="r46-47"];
      "autotest\ntestcases" -> "1681567" [headlabel="r45"];

      node [ color=yellow, shape=ellipse, fontname=Helvetica, fontsize=10, URL="https://sourceforge.net/tracker/?func=detail&group_id=129981&atid=716425&aid=\N" ];
      "1681567" [ label="\N\n(checksum)" ];
      "0.3.1" -> "1681567";
      "1681567" -> "0.3.99.2" [headlabel="r48"];
      "1849644" [ label="\N\n(configure)" ];
      "0.3.1" -> "1849644";
      "1849644" -> "0.3.99.2" [headlabel="r49"];
      "2096705" [ label="\N\n(%lld in query.c)" ];
      "0.3.1" -> "2096705";
      "2096705" -> "0.3.99.2" [headlabel="r44"];
   }
   @enddot
 */

/* this sourcefile is only to provide doc for doxygen to chew on */
