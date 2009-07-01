/**
 * @page FAQ
 * @section AFAQ (Anticipated) FAQs / Standards
 *
 * Frequently-asked Questions, currently "Anticipated FAQs" :)
 * 
 * @subsection Dates
 * <a href="http://www.faqs.org/rfcs/rfc3339.html">rfc-3339</a> is a great neutral way to represent a date
 *
 * @subsection Versions
 * - myproject-0.6.11.12a-pre1-rc2 is really difficult for automated tools to strcmp() accurately.  Also, cannot be used for RPMs.  Why do we need this complexity?  I can hear the machines screaming form here (well, OK, not quite so dramatic).  I've pushed this to a clear, obvious X.Y.Z.M version number.
 *   - epoch?  ...is for wimps! :)
 *   - computers unanimously agree that X+1 \> X, so even sequential numbers of SVN checkins are functional.  Use of decimals is a middleground between the meatware and the hardware. :)
 */

/* this sourcefile is only to provide doc for doxygen to chew on */
