### ---------------------------------------------------------------------------
### --- WDCM Usage Dashboard, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
### --- general
library(shiny)
library(shinydashboard)
library(shinycssloaders)
### --- outputs
library(visNetwork)
library(rbokeh)
library(networkD3)
library(DT)

# - options
options(warn = -1)

shinyUI(
  
  fluidPage(title = 'WDCM Projects', 
            theme = NULL,
            
            # - fluidRow Title
            fluidRow(
              column(width = 12,
                     h2('WDCM Usage Dashboard'),
                     HTML('<font size="3"><b>Wikidata Concepts Monitor</b></font>')
                     
                     )
            ), # - fluidRow Title END
            
            # - fluidRow Logo
            fluidRow(
              column(width = 12,
                     img(src='Wikidata-logo-en.png', 
                         align = "left")
                     )
            ), # - fluidRow END
            
            # - hr()
            fluidRow(
              column(width = 12,
                     hr()
                     )
            ),
            
            # - fluidRow Boxes
            fluidRow(
              column(width = 12,
                     tabBox(id = 'MainBox', 
                            selected = 'Dahsboard', 
                            title = '', 
                            width = 12,
                            height = NULL, 
                            side = 'left',
                            
                            # - tabPanel Dahsboard
                            tabPanel("Dahsboard",
                                     fluidRow(
                                       column(width = 12,
                                       hr(),
                                       tabBox(width = 12,
                                              title = '',
                                              id = "Usage",
                                              selected = "Usage",
                                              tabPanel(title = "Usage",
                                                       id = "usage",
                                                       fluidRow(
                                                         column(width = 6,
                                                                br(),
                                                                HTML('<font size=2><b>Note:</b> This page follows a columnar organization: <i>Basic Facts</i> and <i>Categories</i> to the left, and <i>Projects</i> to the right. 
                                                                     The Dashboard will initialize a random choice of <i>Category</i> and <i>Project</i> in the <b>Category Report</b> and <b>Project Report</b> areas, 
                                                                     respectively. Use the selection and search fields to select a category or a project that you want to generate a Report 
                                                                     for.</font>')
                                                                )
                                                         ),
                                                       # - fluidRow: ValueBoxes
                                                       fluidRow(
                                                         
                                                         column(width = 6,
                                                                fluidRow(
                                                                   column(width = 12,
                                                                          fluidRow(
                                                                            column(width = 12,
                                                                              h3('Basic Facts'), 
                                                                              HTML('The total Wikidata item usage, how many sister projects have a client-side Wikidata usage tracking enabled, 
                                                                              how many Wikidata semantic categories of items are encompassed by this analysis, and how many different 
                                                                              Project Types.')
                                                                              )
                                                                            ),
                                                                          fluidRow(
                                                                            column(width = 3,
                                                                                   withSpinner(infoBoxOutput("totalUsageBox", width = 12), size = .5)
                                                                                   ),
                                                                            column(width = 3,
                                                                                   withSpinner(valueBoxOutput("totalProjectsBox", width = 12), size = .5)
                                                                                   ),
                                                                            column(width = 3,
                                                                                   withSpinner(valueBoxOutput("totalCategoriesBox", width = 12), size = .5)
                                                                                   ),
                                                                            column(width = 3,
                                                                                   withSpinner(valueBoxOutput("totalProjectTypesBox", width = 12), size = .5)
                                                                                   )
                                                                            ),
                                                                          br(), br()
                                                                          )
                                                                   ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr(),
                                                                         h3('Category Report'), 
                                                                         HTML('Wikidata usage overview for a specific category, including the distribution of usage in category accross projects 
                                                                              and the top Wikidata items per category.'),
                                                                         br(), br(),
                                                                         selectizeInput('categories',
                                                                                        'Select category:',
                                                                                        choices = NULL,
                                                                                        multiple = FALSE
                                                                         ),
                                                                         br(), br(),
                                                                         fluidRow(
                                                                           column(width = 12,
                                                                                  htmlOutput('categoryProjects_overview_Title'),
                                                                                  br(), br(),
                                                                                  withSpinner(plotOutput('categoryProjects_overview',
                                                                                                         width = "700px",
                                                                                                         height = "450px")
                                                                                  )
                                                                           )
                                                                         ),
                                                                         fluidRow(
                                                                           column(width = 12,
                                                                                  br(), br(),
                                                                                  htmlOutput('categoryItems_overview_Title'),
                                                                                  HTML("<font size = 2><b>Note: </b>In the absence of English item label the Wikidata item ID 
                                                                                                is used in place of it.</font>"),
                                                                                  br(), br(),
                                                                                  withSpinner(plotOutput('categoryItems_overview',
                                                                                                         width = "700px",
                                                                                                         height = "600px")
                                                                                  )
                                                                           )
                                                                         )
                                                                         )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr(),
                                                                         h3('Categories General Overview'),
                                                                         HTML('<b>Wikidata item usage per semantic category</b><br>
                                                                                 <font size="2"><b>Note:</b> The current selection of semantic categories does not 
                                                                              encompass all Wikidata items.</font>'),
                                                                         br(), br(),
                                                                         withSpinner(plotOutput('basicFacts_CategoryLine',
                                                                                                width = "700px",
                                                                                                height = "500px")
                                                                         )
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         br(), br(),
                                                                         HTML('<b>Wikidata item usage per semantic category in each project type</b><br>
                                                                              <font size="2"><b>Note:</b> Item usage count is given on a logarithmic scale.</font>'),
                                                                         br(), br(),
                                                                         withSpinner(plotOutput('basicFacts_ProjectTypeCategory',
                                                                                                width = "700px",
                                                                                                height = "900px")
                                                                                     )
                                                                         )
                                                                  )
                                                                ),
                                                         
                                                         column(width = 6,
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         fluidRow(
                                                                           column(width = 12,
                                                                                  h3('Project Report'), 
                                                                                  HTML('Wikidata usage overview for a specific project, including: the distribution of usage in Wikidata semantic categories, 
                                                                                       total Wikidata usage volume, similar projects, and the top Wikidata items.'),
                                                                                  br(), br(),
                                                                                  selectizeInput('projects',
                                                                                                 'Search projects:',
                                                                                                 choices = NULL,
                                                                                                 multiple = FALSE
                                                                                                 ),
                                                                                  br(), br(),
                                                                                  fluidRow(
                                                                                    column(width = 4,
                                                                                           withSpinner(htmlOutput('projectOverview_Report'))
                                                                                           ),
                                                                                    column(width = 8,
                                                                                      withSpinner(plotOutput('projectOverview_Category',
                                                                                                             width = "550px",
                                                                                                             height = "450px")
                                                                                                  )
                                                                                      )
                                                                                    ),
                                                                                  fluidRow(
                                                                                    column(width = 12,
                                                                                           br(), br(),
                                                                                           htmlOutput('projectOverview_relativeRank_Title'),
                                                                                           br(), br(),
                                                                                           withSpinner(plotOutput('projectOverview_relativeRank',
                                                                                                                  width = "800px",
                                                                                                                  height = "350px")
                                                                                                       )
                                                                                           )
                                                                                    ),
                                                                                  fluidRow(
                                                                                    column(width = 12,
                                                                                           br(), br(),
                                                                                           htmlOutput('projectOverview_semantics_Title'),
                                                                                           HTML("<font size = 2><b>Note: </b>We study the distribution of Wikidata usage across the semantic categories to 
                                                                                                determine which client projects use Wikidata in a similar way. In this graph, each project points towards the one 
                                                                                                most similar to it. The selected projects has a different color. The results are relevant only in the context 
                                                                                                of the current selection: the selected project + its 20 nearest semantic neighboors.</font>"),
                                                                                           withSpinner(visNetwork::visNetworkOutput('projectOverview_semantics', 
                                                                                                                                    height = 500))
                                                                                           )
                                                                                    
                                                                                  ),
                                                                                  fluidRow(
                                                                                    column(width = 12, 
                                                                                           br(), br(),
                                                                                           htmlOutput('projectOverview_topItems_Title'),
                                                                                           HTML("<font size = 2><b>Note: </b>In the absence of English item label the Wikidata item ID 
                                                                                                is used in place of it.</font>"),
                                                                                           br(), br(),
                                                                                           withSpinner(plotOutput('projectOverview_topItems',
                                                                                                                  width = "700px",
                                                                                                                  height = "600px")
                                                                                           )
                                                                                    )
                                                                                  )
                                                                                  )
                                                                           )
                                                                         )
                                                                  )
                                                         )
                                                       )
                                                       
                                                       ), # - tabPanel BasicFacts END
                                              
                                              tabPanel(title = "Tabs/Crosstabs",
                                                       id = "tabs",
                                                       fluidRow(
                                                         column(width = 12,
                                                                fluidRow(
                                                                  column(width = 6,
                                                                         br(),
                                                                         HTML('<font size = 2>Here you can make <b>selections</b> of client projects and semantic categories to learn about Wikidata 
                                                                              usage across them.<br> <b>Note:</b> You can search and add projects into the <i>Search projects</i> field by 
                                                                              using (a) <b>project names</b> (e.g. <i>enwiki</i>, <i>dewiki</i>, <i>sawikiquote</i>, and similar or (b) by using 
                                                                              <b>project types</b> that start with <b>"_"</b> (underscore, e.g. <i>_Wikipedia</i>, <i>_Wikisource</i>, <i>_Commons</i>, and 
                                                                              similar; try typing anything into the Select projects field that starts with an underscore). Please note that by selecting 
                                                                              a project type (again: <i>_Wikipedia</i>, <i>_Wikiquote</i>, and similar) you are selecting <b>all</b> client 
                                                                              projects of the respective type, and that\'s potentially a lot of data. The Dashboard will pick unique 
                                                                              projects from whatever you have inserted into the Search projects field. The selection of projects will be intesected 
                                                                              with the selection of semantic categories from the Select categories field, and the obtained results will refer only 
                                                                              to the Wikidata items from the current selection of client projects <i>and</i> semantic categories. 
                                                                              In other words: <i>disjunction</i> operates inside the two search fields, while <i>conjunction</i> operates 
                                                                              across the two search fields.<br> <b>Note:</b> The Dashboard will initialize a choice of three project types 
                                                                              (<i>Wikipedia</i>, <i>Wikinews</i>, and <i>Wiktionary</i>) and a random choice of six semantic categories. All charts will present at 
                                                                              most 25 top projects in respect to the Wikidata usage and relative to the current selection; however, <b>complete 
                                                                              selection data sets</b> are available for download (<i>.csv</i>) beneath each chart.</font>'),
                                                                         br(), br()
                                                                         )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 3,
                                                                         selectizeInput('selectProject',
                                                                                        'Search projects:',
                                                                                        choices = NULL,
                                                                                        multiple = TRUE)
                                                                         ),
                                                                  column(width = 3,
                                                                         selectizeInput('selectCategories',
                                                                                        'Search categories:',
                                                                                        choices = NULL,
                                                                                        multiple = TRUE)
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 2,
                                                                         actionButton('applySelection',
                                                                                      label = "Apply Selection",
                                                                                      width = '70%',
                                                                                      icon = icon("database", 
                                                                                                  class = NULL, 
                                                                                                  lib = "font-awesome")
                                                                                      )
                                                                         )
                                                                  ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr()
                                                                         )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 6,
                                                                         h4('Projects'),
                                                                         withSpinner(plotOutput('tabulations_projectsChart', 
                                                                                                height = "600px")),
                                                                         downloadButton('tabulations_projectsDownload_Frame',
                                                                                        'Data (csv)')
                                                                         ),
                                                                  column(width = 6,
                                                                         h4('Categories'),
                                                                         withSpinner(plotOutput('tabulations_categoriesChart', 
                                                                                                height = "600px")),
                                                                         downloadButton('tabulations_categoriesDownload_Frame',
                                                                                        'Data (csv)')
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr()
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 6,
                                                                         h4('Project Types'),
                                                                         withSpinner(plotOutput('tabulations_projectTypesChart', 
                                                                                                height = "600px")),
                                                                         downloadButton('tabulations_projectTypesChart_Frame',
                                                                                        'Data (csv)')
                                                                  ),
                                                                  column(width = 6
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr()
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         h4('Project vs Categories'),
                                                                         withSpinner(plotOutput('crosstabulations_projectsCategoriesChart', 
                                                                                                height = "850px")),
                                                                         downloadButton('crosstabulations_projectsCategoriesFrame',
                                                                                        'Data (csv)')
                                                                         )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         hr()
                                                                  )
                                                                ),
                                                                fluidRow(
                                                                  column(width = 12,
                                                                         h4('Project Types vs Categories'),
                                                                         withSpinner(plotOutput('crosstabulations_projectTypesCategoriesChart', 
                                                                                                height = "850px")),
                                                                         downloadButton('crosstabulations_projectTypeCategoriesChartFrame',
                                                                                        'Data (csv)')
                                                                  )
                                                                )
                                                                )
                                                         ),
                                                       fluidRow(
                                                         column(width = 12,
                                                                hr()
                                                                )
                                                         )
                                                       ), # - tabPanel Tabs/Crosstabs END
                                              
                                              tabPanel(title = "Tables",
                                                       id = "tables",
                                                       fluidRow(
                                                         column(width = 6,
                                                                br(),
                                                                HTML('<font size = 2>Here you can access <b> some tabulated and cross-tabulated raw data</b> on Wikidata usage. <br> 
                                                                     All tables can be searched and sorted by any of the respective columns.</font>'),
                                                                br(), br()
                                                         )
                                                       ),
                                                       fluidRow(
                                                         column(width = 4,
                                                                HTML('<font size = 2><b>Table A. Project Totals.</b></font>'),
                                                                br(), br(),
                                                                withSpinner(DT::dataTableOutput('projectTable', width = "100%"))
                                                                ),
                                                         column(width = 4,
                                                                HTML('<font size = 2><b>Table B. Category Totals.</b></font>'),
                                                                br(), br(),
                                                                withSpinner(DT::dataTableOutput('CategoryTable', width = "100%"))
                                                         ),
                                                         column(width = 4,
                                                                HTML('<font size = 2><b>Table C. Project vs Category Cross-Tabulation.</b></font>'),
                                                                br(), br(),
                                                                withSpinner(DT::dataTableOutput('projectCategoryDataTable', width = "100%"))
                                                         )
                                                       ),
                                                       fluidRow(
                                                         column(width = 12,
                                                                hr()
                                                                )
                                                       ),
                                                       fluidRow(
                                                         column(width = 4,
                                                                HTML('<font size = 2><b>Table D. Project Type Totals.</b></font>'),
                                                                br(), br(),
                                                                withSpinner(DT::dataTableOutput('projectType', width = "100%"))
                                                         ),
                                                         column(width = 6,
                                                                HTML('<font size = 2><b>Table E. Project Type vs Category Cross-Tabulation.</b></font>'),
                                                                br(), br(),
                                                                withSpinner(DT::dataTableOutput('projectTypeCategory', width = "100%"))
                                                         )
                                                       )
                                              )
                                              ) # - tabBox: Wikidata Usage END
                                       )
                                       )
                                     
                                     ), # - tabPanel Dashboard END
                            
                                      # - tabPanel Description
                                      tabPanel("Description",
                                               fluidRow(
                                                 column(width = 8,
                                                        HTML('<h2>WDCM Usage Dashboard</h2>
                                                             <h4>Description<h4>
                                                             <hr>
                                                             <h4>Introduction<h4>
                                                             <br>
                                                             <p><font size = 2>This Dashboard is a part of the <b>Wikidata Concepts Monitor (WDMC)</b>. The WDCM system provides analytics on Wikidata usage
                                                             across the Wikimedia sister projects. The WDCM Usage Dashboard focuses on providing the detailed statistics on Wikidata usage in particular sister projects or 
                                                             the selected subsets of them. Three pages that present analytical results in this Dashboard receive a description here: (1) <b><i>Usage</i></b>, (2) <b><i>Tabs/Crosstabs</i></b>, 
                                                             and (3) <b><i>Tables</i></b>. But first, definitions.</font></p>
                                                             <hr>    
                                                             <h4>Definitions</h4>
                                                             <br>
                                                             <p><font size = 2><b>N.B.</b> The current <b>Wikidata item usage statistic</b> definition is <i>the count of the number of pages in a particular client project
                                                             where the respective Wikidata item is used</i>. Thus, the current definition ignores the usage aspects completely. This definition is motivated by the currently 
                                                             present constraints in Wikidata usage tracking across the client projects 
                                                             (see <a href = "https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage" target = "_blank">Wikibase/Schema/wbc entity usage</a>). 
                                                             With more mature Wikidata usage tracking systems, the definition will become a subject 
                                                             of change. The term <b>Wikidata usage volume</b> is reserved for total Wikidata usage (i.e. the sum of usage statistics) in a particular 
                                                             client project, group of client projects, or semantic categories. By a <b>Wikidata semantic category</b> we mean a selection of Wikidata items that is 
                                                             that is operationally defined by a respective SPARQL query, returning a selection of items that intuitivelly match a human, natural semantic category. 
                                                             The structure of Wikidata does not necessarily match any intuitive human semantics. In WDCM, an effort is made to select the semantic categories so to match 
                                                             the intuitive, everyday semantics as much as possible, in order to assist anyone involved in analytical work with this system. However, the choice of semantic 
                                                             categories in WDCM is not necessarily exhaustive (i.e. they do not necessarily cover all Wikidata items), neither the categories are necessarily 
                                                             mutually exclusive. The Wikidata ontology is very complex and a product of work of many people, so there is an optimization price to be paid in every attempt to 
                                                             adapt or simplify its present structure to the needs of a statistical analytical system such as WDCM. The current set of WDCM semantic categories is thus not 
                                                             normative in any sense and a subject  of change in any moment, depending upon the analytical needs of the community.</font></p>
                                                             <p><font size = 2>The currently used <b>WDCM Taxonomy</b> of Wikidata items encompasses the following 14 semantic categories: <i>Geographical Object</i>, <i>Organization</i>, <i>Architectural Structure</i>, 
                                                             <i>Human</i>, <i>Wikimedia</i>, <i>Work of Art</i>, <i>Book</i>, <i>Gene</i>, <i>Scientific Article</i>, <i>Chemical Entities</i>, <i>Astronomical Object</i>, <i>Thoroughfare</i>, <i>Event</i>, 
                                                             and <i>Taxon</i>.</font></p>
                                                             <hr>
                                                             <h4>Usage</h4>
                                                             <br>
                                                             <p><font size = 2>The Usage tab provides elementary statistics on Wikidata usage across the semantic categories (left column) and sister projects 
                                                             (right column).<br>
                                                             <b><i>To the left</b></i>, we first encounter a general overview of <i>Basic Facts</i>: the number of Wikidata items that are encompassed by the current WDCM taxonomy (in effect, 
                                                             this is the number of items that are encompassed by all WDCM analyses), the number of sister projects that have client-side Wikidata usage tracking enabled (currently, 
                                                             that means that the <a href = "https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage" target = "_blank">Wikibase/Schema/wbc entity usage</a>) is present there), 
                                                             the number of semantic categories in the current version of the WDCM Taxonomy, and the number of different sister project types (e.g. <i>Wikipedia</i>, <i>Wikinews</i>, etc).
                                                             <br>
                                                             The <b>Category Report</b> subsection allows you to select a specific semantic category and generate two charts beneath the selection: (a) the category top 30 projects chart, and 
                                                             (b) the category top 30 Wikidata items chart. The first chart will display 30 sister projects that use Wikidata items from this semantic category the most, with the usage data 
                                                             represented on the horizontal axis, and the project labels on the vertical axis. The percentages next to the data points in this chart refer to the proportion of total category usage 
                                                             that takes place in the respective project. The next chart will display the 30 most popular items from the selected semantic category: item usage is again placed on the horizontal axis, 
                                                             item labels are on the vertical axis, and item IDs are placed next to the data points themselves.
                                                             <br>
                                                             The <b>Categories General Overview</b> subsection is static and allows no selection; it introduces two concise overviews of Wikidata usage across the semantic categories of 
                                                             Wikidata items. The <i>Wikidata Usage per Semantic Cateory</i> chart provides semantic categories on the vertical and item usage statistics on the horizontal axis; the percentages 
                                                             tells us about the proportion of total Wikidata usage that the respective semantic category carries. Beneath, the <i>Wikidata item usage per semantic category in each project type</i> 
                                                             provides a cross-tabulation of semantic categories vs. sister project types. The categories are color-coded and represented on the horizontal axes, while each chart represents one project 
                                                             type. The usage scale, represented on the vertical axes, is logarithmic to ease the comparison and enable practical data visualization.
                                                             <br>
                                                             <b><i>To the right</b></i>, an opportunity to inspect Wikidata usage in a single Wikimedia project is provided. The <b>Project Report</b> section allows you to select a single Wikimedia 
                                                             project and obtain results on it. The first section that will be generated upon making a selection provides a concise narrative summary of Wikidata usage in the selected project alongside 
                                                             a chart presenting an overview of Wikidata usage per semantic category. The next chart, <i>Wikidata usage rank</i>, show the rank position of the selected project among other sister projects 
                                                             in respect to the Wikidata usage volume. Beneath, a more complex structure, <i>Semantic Neighbourhood</i>, is given. In this network, or a directed graph if you prefere, each project points 
                                                             towards the one most similar to it. The selected projects has a different color. The results are relevant only in the context of the current selection: the selected project and its 20 nearest 
                                                             semantic neighboors only are presented. Once again: each project points to the one which utilizes Wikidata in a way most similar to it. The <i>top 30 Wikidata items</i> chart presents the top 30   
                                                             Wikidata items in the selected project: item labels are given on the vertical axis, Wikidata usage on the horizontal axis, and the item IDs are labeled close to the data points themselves.
                                                             </font></p>
                                                             <hr>
                                                             <h4>Tabs/Crosstabs</h4>
                                                             <br>
                                                             <p><font size = 2>
                                                             Here we have the most direct opportunity to study the Wikidata usage statistics across the sister projects. A selection of projects and semantic categories will be intersected and only results in 
                                                             the scope of the intersection will be returned. The charts should be self-explanatory: the usage statistic is always represented by the vertical axis, while the horizontal axis and sub-panels play 
                                                             various roles in the context of whether a category vs project or a category vs project type crosstabulation is provided. Data points are labeled in million (M) or thousand (K) pages (see Wikidata usage) 
                                                             definition above). While charts can display a limited number of data points only, relative to the size of the selection, each of them is accompanied by a <b>Data (csv)</b> button that will initiate a 
                                                             download of the full respective data set as a comma separated file.  
                                                             </font></p>
                                                             <hr>
                                                             <h4>Tables</h4>
                                                             <br>
                                                             <p><font size = 2>The section presents searchable and sortable tables and crosstabulations with self-explanatory semantics. Access full WDCM usage datasets from here.</font></p>
                                                             
                                                             ')
                                                 )
                                               )
                                               ), # - tabPanel Usage END
                                      
                                      # - tabPanel Navigate
                                      tabPanel("Navigate WDCM", 
                                               fluidRow(
                                                 column(width = 8,
                                                        HTML('<h2>WDCM Navigate</h2>
                                                   <h4>Your orientation in the WDCM Dashboards System<h4>
                                                   <hr>
                                                   <ul>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/">WDCM Portal</a></b>.<br>
                                                   <font size = "2">The entry point to WDCM Dashboards.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_OverviewDashboard/">WDCM Overview</a></b><br>
                                                   <font size = "2">The big picture. Fundamental insights in how Wikidata is used across the client projects.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_SemanticsDashboard/">WDCM Semantics</a></b><br>
                                                   <font size = "2">Detailed insights into the WDCM Taxonomy (a selection of semantic categories from Wikidata), its distributional
                                                   semantics, and the way it is used across the client projects. If you are looking for Topic Models - that&#8217;s where
                                                   they live.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_UsageDashboard/">WDCM Usage</a> (current dashboard)</b><br>
                                                   <font size = "2">Fine-grained information on Wikidata usage across client projects and project types. Cross-tabulations and similar..</font></li><br>
                                                   <li><b>WDCM Items</b><br>
                                                   <font size = "2">Fine-grained information on particular Wikidata item usage across the client projects.<b> (Under development)</b></font></li><br>
                                                  <li><b><a href = "https://wikitech.wikimedia.org/wiki/Wikidata_Concepts_Monitor">WDCM System Technical Documentation</a></b><br>
                                                   <font size = "2">The WDCM Wikitech Page.</font></li>
                                                   </ul>'
                                                        )
                                                 )
                                               )
                                               ) # - tabPanel Structure END
                            
                            ) # - tabBox END
                     
                     ) # - main column of fluidRow Boxes END
              
              ), # - # - fluidRow Boxes END
            
            # - fluidRow Footer
            fluidRow(
              column(width = 12,
                     hr(),
                     HTML('<b>Wikidata Concepts Monitor :: WMDE 2017</b><br>Diffusion: <a href="https://phabricator.wikimedia.org/diffusion/AWCM/" target = "_blank">WDCM</a><br>'),
                     HTML('Contact: Goran S. Milovanovic, Data Scientist, WMDE<br>e-mail: goran.milovanovic_ext@wikimedia.de
                          <br>IRC: goransm'),
                     br(),
                     br()
                     )
            ) # - fluidRow Footer END
            
            ) # - fluidPage END
  
) # - ShinyUI END
