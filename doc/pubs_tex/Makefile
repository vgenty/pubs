
DOC=PUBS

all:
	@pdflatex $(DOC).tex
	@bibtex   $(DOC) 
	@pdflatex $(DOC).tex
	@pdflatex $(DOC).tex
	@rm -f $(DOC).bbl
	@rm -f $(DOC).blg
	@rm -f $(DOC).toc
	@rm -f $(DOC).log
	@rm -f $(DOC).out
	@rm -f $(DOC).aux
clean:
	@rm -f $(DOC).pdf

