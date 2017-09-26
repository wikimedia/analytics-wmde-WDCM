### ---------------------------------------------------------------------------
### --- WDCM Usage Dashboard, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list=ls())
### --- general
library(shiny)
library(shinydashboard)
library(shinycssloaders)
### --- outputs
library(visNetwork)
library(rbokeh)
library(networkD3)
library(ggvis)
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
                     HTML('<font size="3"><b>Wikidata Concepts Monitor</b></font>'),
                     hr()
                     )
            ), # - fluidRow Title END
            
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
                                                                              across the two search fields.<br> <b>Note:</b> The Dashboard will initialize a random choice of five 
                                                                              projects and three semantic categories. All charts will present at most 25 top projects in respect to the 
                                                                              Wikidata usage and relative to the current selection; however, <b>complete selection data sets</b> are available for 
                                                                              download (<i>.csv</i>) beneath each chart.</font>'),
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
                                                                         plotOutput('tabulations_projectsChart'),
                                                                         downloadButton('tabulations_projectsDownload_Frame',
                                                                                        'Data (csv)')
                                                                         ),
                                                                  column(width = 6,
                                                                         h4('Categories'),
                                                                         plotOutput('tabulations_categoriesChart'),
                                                                         downloadButton('tabulations_categoriesDownload_Frame',
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
                                                        HTML('<h2>WDCM Overview Dashboard</h2>
                                                             <h4>Description<h4>
                                                             <hr>
                                                             <h4>Introduction<h4>
                                                             <br>
                                                             <p><font size = 2>This Dashboard is a part of the <b>Wikidata Concepts Monitor (WDMC)</b>. The WDCM system provides analytics on Wikidata usage
                                                             across the client projects. The WDCM Overview Dashboard presents the big picture of Wikidata usage; other WDCM dashboards go
                                                             into more detail. The Overview Dashboard provides insights into <b>(1)</b> the similarities between the client projects in respect to their use of 
                                                             of Wikidata, as well as <b>(2)</b> the volume of Wikidata usage in every client project, <b>(3)</b> Wikidata usage tendencies, described by the volume of 
                                                             Wikidata usage in each of the semantic categories of items that are encompassed by the current WDCM edition, <b>(4)</b> the similarities between the 
                                                             Wikidata semantic categories of items in respect to their usage across the client projects, <b>(5)</b> ranking of client projects in respect to their 
                                                             Wikidata usage volume, <b>(6)</b> the Wikidata usage breakdown across the types of client projects and Wikidata semantic categories.</font></p>
                                                             <hr>
                                                             <h4>Definitions</h4>
                                                             <br>
                                                             <p><font size = 2><b>N.B.</b> The current <b>Wikidata item usage statistic</b> definition is <i>the count of the number of pages in a particular client project
                                                             where the respective Wikidata item is used</i>. Thus, the current definition ignores the usage aspects completely. This definition is motivated by the currently 
                                                             present constraints in Wikidata usage tracking across the client projects. With more mature Wikidata usage tracking systems, the definition will become a subject 
                                                             of change. The term <b>Wikidata usage volume</b> is reserved for total Wikidata usage (i.e. the sum of usage statistics) in a particular 
                                                             client project, group of client projects, or semantic categories. By a <b>Wikidata semantic category</b> we mean a selection of Wikidata items that is 
                                                             that is operationally defined by a respective SPARQL query returning a selection of items that intuitivelly match a human, natural semantic category. 
                                                             The structure of Wikidata does not necessarily match any intuitive human semantics. In WDCM, an effort is made to select the semantic categories so to match 
                                                             the intuitive, everyday semantics as much as possible, in order to assist anyone involved in analytical work with this system. However, the choice of semantic 
                                                             categories in WDCM is not necessarily exhaustive (i.e. they do not necessarily cover all Wikidata items), neither the categories are necessarily 
                                                             mutually exclusive. The Wikidata ontology is very complex and a product of work of many people, so there is an optimization price to be paid in every attempt to 
                                                             adapt or simplify its present structure to the needs of a statistical analytical system such as WDCM. The current set of WDCM semantic categories is thus not 
                                                             normative in any sense and a subject  of change in any moment, depending upon the analytical needs of the community.</font></p>
                                                             <hr>
                                                             <h4>Wikidata Usage Overview</h4>
                                                             <br>
                                                             <p><font size = 2>The similarity structure in Wikidata usage <i>across the client projects</i> is presented. Each bubble represents a client project.
                                                             The size of the bubble reflects the volume of Wikidata usage in the respective project. Projects similar in respect to the semantics of Wikidata
                                                             usage are grouped together.<br>
                                                             The bubble chart is produced by performing a <a href="https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding" target="_blank">t-SNE dimensionality reduction</a> 
                                                             of the client project pairwise Euclidean distances derived from the Projects x Categories contingency table. Given that the original higher-dimensional space 
                                                             from which the 2D map is derived is rather constrained by the choice of a small number of semantic categories, the similarity mapping is somewhat 
                                                             imprecise and should be taken as an attempt at an approximate big picture of the client projects similarity structure only. More precise 2D maps of 
                                                             the similarity structures in client projects are found on the WDCM Semantics Dashboard, where each semantic category first receives an 
                                                             <a href = "https://en.wikipedia.org/wiki/Topic_model" target = "_blank">LDA Topic Model</a>, 
                                                             and the similarity structure between the client projects is then derived from project topical distributions.<br>
                                                             While the <i>Explore</i> tab presents a dynamic <a href = "http://hafen.github.io/rbokeh/" target="_blank">{Rbokeh}</a> visualization alongised 
                                                             the tools to explore it in detail, the <i>Highlights</i> tab shows a static <a href = "http://ggplot2.org/" target="_blank">{ggplot2}</a> plot with the most important client projects 
                                                             marked (<b>NOTE.</b> Only top five projects (of each project type) in respect to Wikidata usage volume are labeled).</font></p>
                                                             <hr>
                                                             <h4>Wikidata Usage Tendency</h4>
                                                             <br>
                                                             <p><font size = 2>The similarity structure in Wikidata usage <i>across the semantic categories</i> is presented. Each bubble represents a Wikidata semantic
                                                             category. The size of the bubble reflects the volume of Wikidata usage from the respective category. If two categories are found in proximity,
                                                             that means that the projects that tend to use the one also tend to use the another, and vice versa. Similarly to the Usage Overview, the 2D mapping is obtained by performing 
                                                             a <a href="https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding" target="_blank">t-SNE dimensionality reduction</a> 
                                                             of the categories pairwise Euclidean distances derived from the Projects x Categories contingency table. </font></p>
                                                             <hr>
                                                             <h4>Wikidata Usage Distribution</h4>
                                                             <br>
                                                             <p><font size = 2>The plots are helpful to build an understanding of the relative range of Wikidata usage across the client projects.
                                                             In the <i>Project Usage Rank-Frequency</i> plot, each point represents a client project; Wikidata usage is represented on the vertical and
                                                             the project usage rank on the horizontal axis, while only top project (per project type) are labeled. The highly-skewed, asymmetrical
                                                             distribution reveals that a small fraction of client projects only accounts for a huge proportion of Wikidata usage.<br> In the
                                                             <i>Project Usage log(Rank)-log(Frequency)</i> plot, the logarithms of both variables are represented. 
                                                             A <a href = "https://en.wikipedia.org/wiki/Power_law" target="_blank">power-law</a> relationship holds true if this
                                                             plot is linear. The plot includes the best linear fit, however, no attempts to estimate the underlying probability distribution were made. </font></p>
                                                             <hr>
                                                             <h4>Client Project Types</h4>
                                                             <br>
                                                             <p><font size = 2>Project types are provided in the rows of this chart, while the semantic categories are given on the horizontal axis.
                                                             The height of the respective bar indicates Wikidata usage volume from the respective semantic category in a particular client project.</font></p>
                                                             <hr>
                                                             <h4>Client Projects Usage Volume</h4>
                                                             <br>
                                                             <p><font size = 2>Use the slider to select the percentile rank range of the Wikidata usage volume distribution across the client project to show. The
                                                             chart will automatically adjust to present the selected projects in increasing order of Wikidata usage, and presenting at most 30 top projects
                                                             from the selection. <b>NOTE.</b> The <a href="https://en.wikipedia.org/wiki/Percentile_rank" target="_blank">percentile rank</a> 
                                                             of a score is the percentage of scores in its frequency distribution that are equal to or lower than it. 
                                                             For example, a client project that has a Wikidata usage volume greater than or equal to 75% of all client projects under
                                                             consideration is said to be at the 75th percentile, where 75 is the percentile rank.<br> In effect, you can browse the whole 
                                                             distribution of Wikidata usage across the client projects by selecting the lower and uppers limit in terms of usage percentile rank.</font></p>
                                                             <hr>
                                                             <h4>Wikidata Usage Browser</h4>
                                                             <br>
                                                             <p><font size = 2>A breakdown of Wikidata usage statistics across client projects and semantic categories. To the left, 
                                                             a table that presents a Client Project vs. Semantic Category cross-tabulation. The Usage column in this table is the Wikidata 
                                                             usage statistic for a particular Semantic Category x Client Project combination (e.g. The Wikidata usage in the category "Human" in 
                                                             the dewiki project). To the right, the total Wikidata usage per client project is presented (i.e. the sum of Wikidata usage across 
                                                             all semantic categories for a particular client project; e.g. the total Wikidata usage volume of enwiki).</font></p>
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
                                                             <li><b>WDCM Overview</b> (current dashboard).<br>
                                                             <font size = "2">The big picture. Fundamental insights in how Wikidata is used across the client projects.</font></li><br>
                                                             <li><b>WDCM Semantics.</b><br>
                                                             <font size = "2">Detailed insights into the WDCM ontology (a selection of semantic categories from Wikidata), its distributional
                                                             semantics, and the way it is used across the client projects. If you are looking for Topic Models, yes that&#8217;s where
                                                             they live in WDCM.</font></li><br>
                                                             <li><b>WDCM Usage.</b><br>
                                                             <font size = "2">Fine-grained information on Wikidata usage across client projects and project types. Cross-tabulations and similar..</font></li><br>
                                                             <li><b>WDCM Items</b><br>
                                                             <font size = "2">Fine-grained information on particular Wikidata item usage across the client projects..</font></li><br>
                                                             <li><b>WDCM System Technical Documentation.</b><br>
                                                             <font size = "2">A document that will come to existence eventually. There are rumours of an existing draft.</font></li>
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
                     HTML('<b>Wikidata Concepts Monitor :: WMDE 2017</b><br>GitHub: <a href="https://github.com/wmde/WDCM" target = "_blank">WDCM</a><br>'),
                     HTML('Contact: Goran S. Milovanovic, Data Analyst, WMDE<br>e-mail: goran.milovanovic_ext@wikimedia.de
                          <br>IRC: goransm'),
                     br(),
                     br()
                     )
            ) # - fluidRow Footer END
            
            ) # - fluidPage END
  
) # - ShinyUI END
