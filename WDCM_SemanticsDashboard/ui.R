### ---------------------------------------------------------------------------
### --- WDCM Semantics Dashboard, v. Beta 0.1
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
                     h2('WDCM Semantics Dashboard'),
                     HTML('<font size="3"><b>Wikidata Concepts Monitor</b></font>')
                     
              )
            ), # - fluidRow Title END
            
            # - fluidRow Logo
            fluidRow(
              column(width = 12,
                     img(src = 'Wikidata-logo-en.png', 
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
                                                     id = "Semantic Models",
                                                     selected = "Semantic Models",
                                                     tabPanel(title = "Semantic Models",
                                                              id = "semmodels",
                                                              fluidRow(
                                                                column(width = 6,
                                                                       br(),
                                                                       HTML('<font size=2>This page provides an oportunity to study the WDCM semantic models. The WDCM organizes its knowledge of Wikidata usage 
                                                                            into <b>semantic categories</b> and currently uses 14 of them. Each semantic category encompasses a set of Wikidata items that match a particular intuitive, 
                                                                            natural concept (e.g. "Human", "Geographical Object", "Event", etc).<br>
                                                                            The WDCM develops a <b>semantic topic model</b> (see: <a href = "https://en.wikipedia.org/wiki/Topic_model" target = "_blank">Topic Model</a>) for each 
                                                                            semantic category. Each semantic model encompasses a number of topics, or semantic themes. Each topic is characterized by the importance of 
                                                                            Wikidata items from the respective semantic category in that topic. Here you can browse the semantic categories and inspect 
                                                                            the structure of topics that are encompassed by the respective semantic model. You can also learn about the most important projects in a given category 
                                                                            for a given topic from its semantic model.<br>
                                                                            The Dashboard will initialize a random choice of <i>Category</i> and pick the first <i>Topic</i> from its semantic model. Use the drop-down menus to select 
                                                                            a category and a topic from its semantic model. Three outputs will be generated on this page: the Top 50 items chart, the topic similarity network, and the top 50 
                                                                            projects in this topic chart (scroll down).</font>')
                                                                       )
                                                                       ),
                                                              # - fluidRow: Selections
                                                              fluidRow(
                                                                br(),
                                                                column(width = 3,
                                                                       selectizeInput("selectCategory",
                                                                                      "Select Item Category:",
                                                                                      multiple = F,
                                                                                      choices = NULL,
                                                                                      selected = NULL)
                                                                       ),
                                                                column(width = 3,
                                                                       uiOutput("selectCatTopic")
                                                                       )
                                                                ),
                                                              fluidRow(
                                                                column(width = 12, 
                                                                       hr(),
                                                                       h4('Top 50 items in this topic'),
                                                                       HTML('<font size="2">The chart represents the top 50 most important items in this topic. The importance of each item is given by its 
                                                                            probability of being generated by this particular semantic topic (horizontal axis). The items are ranked; the rank numbers next to the labels 
                                                                            on the vertical axis correspond to the rank numbers in parentheses next to data labels that show the item Wikidata IDs.
                                                                            <i>There\'s a game that you can play here:</i> ask yourself what makes this 50 items go together, what makes them similar, what unifying principle 
                                                                            holds them together in the same semantic topic? Do not forget: it is not only about what you know about the World, but also about how our communities use 
                                                                            Wikidata on their respective projects.</font>'),
                                                                       br(), br(),
                                                                       withSpinner(plotOutput('topItemsTopic',
                                                                                              width = "100%",
                                                                                              height = "850px"))
                                                                       )
                                                              ),
                                                              fluidRow(
                                                                column(width = 12, 
                                                                       hr(),
                                                                       h4('Topic similarity network'),
                                                                       HTML('<font size="2">Each bubble represents one among the top 50 most important items in this semantic topic. Each item points towards the 
                                                                            the item to which it is most similar. Similarity between items is derived not only from item importances (i.e. probabilities) in this topic, but 
                                                                            from all topics that are encompasses by this category\'s semantic model. In interpreting the similarities, do not forget that game is not only 
                                                                            about what you know about the World, but also about how different communities use Wikidata. The more similarly the items are used across the 
                                                                            sister projects, the more likely they will group together in this network. You can drag the network and the nodes around and zoom in and out by 
                                                                            your mouse wheel.</font>'),
                                                                       br(), br(),
                                                                       withSpinner(visNetwork::visNetworkOutput('networkItemsTopic', height = 850))
                                                                )
                                                              ),
                                                              fluidRow(
                                                                column(width = 12, 
                                                                       hr(),
                                                                       h4('Top 50 projects in this topic'),
                                                                       HTML('<font size="2">To put it in a nutshell: here you can see what projects use the selected topic from the respective semantic category the most. 
                                                                            The chart represents the top 50 projects in respect to the prominence of the selected topic. In the WDCM topic models, 
                                                                            the usage pattern of any particular semantic category of Wikidata items, in a particular project, can be viewed as a mixture of semantic topics 
                                                                            from the respective category\'s semantic model. Thus, each project\'s usage pattern in a particular semantic category can be expressed 
                                                                            as a set of proportions up to which each topic contributes to it. The horizontal axis represents the proportion (e.g. the probability) of the 
                                                                            selected topic\'s presence in a particular project. Projects are found on the vertical axis, with the rank numbers corresponding to those near 
                                                                            the data points in the chart.</font>'),
                                                                       br(), br(),
                                                                       withSpinner(plotOutput('topProjectsTopic',
                                                                                              width = "100%",
                                                                                              height = "850px"))
                                                                )
                                                              )
                                                              ), # - tabPanel Semantic Models END
                                                     
                                                     tabPanel(title = "Project Semantics",
                                                              id = "projects",
                                                              fluidRow(
                                                                column(width = 12,
                                                                       fluidRow(
                                                                         column(width = 6,
                                                                                br(), 
                                                                                HTML('<font size = 2>Here you can make a selection of projects and learn about the importance of all available semantic topics from 
                                                                                each semantic category in the project(s) of your choice. <b>Note:</b> You can search and add projects into the <i>Search projects</i> field by 
                                                                                using (a) <b>project names</b> (e.g. <i>enwiki</i>, <i>dewiki</i>, <i>sawikiquote</i>, and similar or (b) by using 
                                                                                <b>project types</b> that start with <b>"_"</b> (underscore, e.g. <i>_Wikipedia</i>, <i>_Wikisource</i>, <i>_Commons</i>, and 
                                                                                similar; try typing anything into the Select projects field that starts with an underscore). Please note that by selecting 
                                                                                a project type (again: <i>_Wikipedia</i>, <i>_Wikiquote</i>, and similar) you are selecting <b>all</b> client 
                                                                                projects of the respective type, and that\'s potentially a lot of data. The Dashboard will pick unique 
                                                                                projects from whatever you have inserted into the Search projects field. <br> <b>Note:</b> The Dashboard will initialize with a choice of 
                                                                                all <i>Wikipedia</i> projects. Then you can make a selection of projects of your own and hit <i>Apply Selection</i> to obtain the result.</font>'),
                                                                                br(), br()
                                                                                )
                                                                         )
                                                                       )
                                                              ),
                                                              # - fluidRow: Selections
                                                              fluidRow(
                                                                br(),
                                                                column(width = 6,
                                                                       selectizeInput("selectProject",
                                                                                      "Select Projects:",
                                                                                      multiple = T,
                                                                                      choices = NULL,
                                                                                      selected = NULL, 
                                                                                      width = 800)
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
                                                                       hr(),
                                                                       h4('Semantic Topics in Wikimedia Projects'),
                                                                       HTML('<font size="2">The vertical axes represent the % of topic engagement in this particular selection of Wikimedia projects. <br>
                                                                            <b>Note:</b> Please be remindided that semantic topics are <i>category-specific</i>: each category has its own semantic model, and each 
                                                                            semantic model encompasses a number of topics. To clarify: Topic 1 is not the same thing in two different categories. You can learn about 
                                                                            the content of any semantic topic in any of the semantic categories on the <i>Semantic Models</i> tab - and in fact that is what one should 
                                                                            do <i>before</i> any attempt to interpret the data that are provided here.</font>'),
                                                                       br(), br(),
                                                                       withSpinner(plotOutput('projectTopicImportance',
                                                                                              width = "100%",
                                                                                              height = "1000px"))
                                                                )
                                                              )
                                                              ), # - tabPanel Projects END
                                                     tabPanel(title = "Similarity Maps",
                                                              id = "similarity",
                                                              fluidRow(
                                                                column(width = 12,
                                                                       fluidRow(
                                                                         column(width = 6,
                                                                                br(), 
                                                                                HTML('<font size = 2>Select a semantic category of Wikidata items to take a look at. A 2D map will be generated where each 
                                                                                     project is represented by a bubble, and where the distance between the projects corresponds with the similarity in their 
                                                                                     usage of Wikidata items <i>from the selected category</i>. Think about semantic categories as perspectives from which you 
                                                                                     can take a look at the structure of similarity that holds among the Wikimedia projects in respect to their usage of Wikidata items.</font>'),
                                                                                br(), br()
                                                                                     )
                                                                                )
                                                                         )
                                                                       ),
                                                              # - fluidRow: Category Selection
                                                              fluidRow(
                                                                br(),
                                                                column(width = 3,
                                                                       selectizeInput("selectCategory2",
                                                                                      "Select Category:",
                                                                                      multiple = F,
                                                                                      choices = NULL,
                                                                                      selected = NULL)
                                                                )
                                                              ),
                                                              fluidRow(
                                                                hr(),
                                                                column(width = 12,
                                                                       h4('Similarity Map'),
                                                                       HTML('<font size = 2>Each bubble represents a client project. 
                                                                            The size of the bubble reflects the volume of Wikidata usage in the respective project; a logarithmic scale is used in this plot.<br> 
                                                                            Projects similar in respect to their usage of Wikidata items <i>from the selected category</i> are grouped together. 
                                                                            Use the tools next to the plot legend to explore the plot and hover over bubbles for details.</font>'),
                                                                       hr(),
                                                                       withSpinner(rbokeh::rbokehOutput('overviewPlotDynamic',
                                                                                                        width = "1400px",
                                                                                                        height = "900px")
                                                                       )
                                                                )
                                                              )
                                                              )
                                                     
                                                     ) # - tabBox: Dashboard END
                                              )
                                       )
                                     
                                     ), # - tabPanel Dashboard END
                            
                            # - tabPanel Description
                            tabPanel("Description",
                                     fluidRow(
                                       column(width = 8,
                                              HTML('<h2>WDCM Semantics Dashboard</h2>
                                                   <h4>Description<h4>
                                                   <hr>
                                                   <h4>Introduction<h4>
                                                   <br>
                                                   <p><font size = 2>This Dashboard is a part of the <b>Wikidata Concepts Monitor (WDMC)</b>. The WDCM system provides analytics on Wikidata usage
                                                             across the Wikimedia sister projects. The WDCM Semantics Dashboard is probably the central and the analytically most complicated of all WDCM Dashboards. 
                                                             Here we provide only the necessary basics of distributional semantics needed in order to understand the results of semantic topic modeling presented on this 
                                                             WDCM dashboard. A user who needs to dive deep into the similarity structures between the Wikimedia sister projects in respect to their Wikidata usage patterns 
                                                             will most probably have to do some additional reading first. However, the Dashboard simplifies the presentation of the results as much as possible to make them 
                                                             accessible to any Wikidata user or Wikipedia editor who is not necessarily involved in Data or Cognitive Science. Reading through the <b>WDCM Semantic Topic Models</b> 
                                                             section in this page is <i>highly advised</i> to anyone who has never met semantic topic models or distributional semantics before. Before that, our next stop: Definitions.
                                                   </font></p>
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
                                                   that is operationally defined by a respective SPARQL query returning a selection of items that intuitivelly match a human, natural semantic category. 
                                                   The structure of Wikidata does not necessarily match any intuitive human semantics. In WDCM, an effort is made to select the semantic categories so to match 
                                                   the intuitive, everyday semantics as much as possible, in order to assist anyone involved in analytical work with this system. However, the choice of semantic 
                                                   categories in WDCM is not necessarily exhaustive (i.e. they do not necessarily cover all Wikidata items), neither the categories are necessarily 
                                                   mutually exclusive. The Wikidata ontology is very complex and a product of work of many people, so there is an optimization price to be paid in every attempt to 
                                                   adapt or simplify its present structure to the needs of a statistical analytical system such as WDCM. The current set of WDCM semantic categories is thus not 
                                                   normative in any sense and a subject  of change in any moment, depending upon the analytical needs of the community.</font></p>
                                                   <p>The currently used <b>WDCM Taxonomy</b> of Wikidata items encompasses the following 14 semantic categories: <i>Geographical Object</i>, <i>Organization</i>, <i>Architectural Structure</i>, 
                                                   <i>Human</i>, <i>Wikimedia</i>, <i>Work of Art</i>, <i>Book</i>, <i>Gene</i>, <i>Scientific Article</i>, <i>Chemical Entities</i>, <i>Astronomical Object</i>, <i>Thoroughfare</i>, <i>Event</i>, 
                                                   and <i>Taxon</i>.</p>
                                                   <hr>
                                                   <h4>WDCM Semantic Topic Models</h4>
                                                   <br>
                                                   <h5>Suggested Readings</h5>
                                                   <ul>
                                                   <li><b>Distributional Semantics.</b> In <i>Wikipedia</i>. Retrieved October 24, 2017, from <a href = "https://en.wikipedia.org/wiki/Distributional_semantics" target = "_blank">https://en.wikipedia.org/wiki/Distributional_semantics</a></li>
                                                   <li><b>Topic model.</b> In <i>Wikipedia</i>. Retrieved October 24, 2017, from <a href = "https://en.wikipedia.org/wiki/Topic_model" target = "_blank">https://en.wikipedia.org/wiki/Topic_model</a></li>
                                                   <li><b>Latent Dirichlet allocation.</b> In <i>Wikipedia</i>. Retrieved October 24, 2017, from <a href = "https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation" target = "_blank">https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation</a></li>
                                                   <li><b>Dimensionality reduction.</b> In <i>Wikipedia</i>. Retrieved October 24, 2017, from <a href = "https://en.wikipedia.org/wiki/Dimensionality_reduction" target = "_blank">https://en.wikipedia.org/wiki/Dimensionality_reduction</a></li>
                                                   </ul>
                                                   <p><font size = 2>While <a href = "https://www.wikidata.org/wiki/Wikidata:Main_Page" target = "_blank">Wikidata</a> itself is a <a href = "https://en.wikipedia.org/wiki/Ontology_(information_science)" 
                                                   target = "_blank">semantic ontology</a> with pre-defined and evolving normative rules of description and inference, <b>Wikidata usage</b> is essentialy a social, behavioral phenomenon, 
                                                   suitable for study by means of <a href = "https://en.wikipedia.org/wiki/Machine_learning" target = "_blank">machine learning</a> in the field of <a href = "https://en.wikipedia.org/wiki/Distributional_semantics" 
                                                   target = "_blank">distributional semantics</a>: the analysis and modeling of statistical patterns of occurrence and co-occurence of Wikidata item and property 
                                                   usage across the client projects (e.g. <i>enwiki</i>, <i>frwiki</i>, <i>ruwiki</i>, etc). WDCM thus employes various statistical approaches in an attempt to describe and provide insights from the observable Wikidata 
                                                   usage statistics (e.g. <a href = "https://en.wikipedia.org/wiki/Topic_model" target = "_blank">topic modeling</a>, <a href = "https://en.wikipedia.org/wiki/Cluster_analysis" target = "_blank">clustering</a>, 
                                                   <a href = "https://en.wikipedia.org/wiki/Dimensionality_reduction" target = "_blank">dimensionality reduction</a>, all beyond providing elementary descriptive statistics of Wikidata usage, of course).
                                                   <br><br>
                                                   <b><i>Wikidata Usage Patterns.</b></i> The <i>“golden line”</i> that connects the reasoning behind all WDCM functions can be non-technically described in the following way. Imagine observing the number of times a set of 
                                                   size <b>N</b> of particular Wikidata items was used across some project (<i>enwiki</i>, for example). Example having the same data or other projects as well: for example, if 200 projects are under analysis, then we 
                                                   have <b>200</b> counts for <b>N</b> items in a set, and the data can be desribed by a <b>N x 200</b> matrix (<i>items</i> x <i>projects</i>). Each column of counts, representing the frequency of occurence of all Wikidata 
                                                   entities under consideration across one of the 200 projects under discussion - a vector, obviously - represents a particular <i>Wikidata usage pattern</i>. By inspecting and modeling statistically the usage pattern matrix - 
                                                   a matrix that encompasses all such usage patterns across the projects, or the derived covariance/correlation matrix - many insigths into the similarities between Wikimedia projects items projects (or, more precisely, 
                                                   the similarities between their usage patterns) can be found.
                                                   <br>In essence, the technology and mathematics behind WDCM relies on the same set of practical tools and ideas that support the development of <a href = "https://en.wikipedia.org/wiki/Semantic_search" target = "_blank">semantic search engines</a> 
                                                   and <a href = "https://en.wikipedia.org/wiki/Recommender_system" target = "_blank">recommendation systems</a>, only applied to a specific dataset that encompasses the usage patterns for tens of millions of Wikidata entities across its client projects.</font></p>
                                                   <hr>
                                                   <h4>Dashboard: Semantic Models</h4>
                                                   <br>
                                                   <p><font size = 2>Each of the 14 currently used semantic categories in the WDCM Taxonomy of Wikidata items receives a separate topic model. Each topic model encompasses two or more 
                                                   topics, or semantic themes. Here you can select a semantic category (e.g. "Geographical Object", "Human") and a particular topic from its model. The page will produce three outputs: 
                                                   (1) the <i>Top 50 items in this topic</i> chart, which presents the 50 most important items in the select topic of the selected category\'s topic model, (2) the <i>Topic similarity network</i>, 
                                                   which presents the similarity structure among the 50 most important items in the selected topic, and (c) the <i>Top 50 projects in this topic</i> chart, where 50 Wikimedia projects in which the 
                                                   selected topic plays a prominent role in the selected semantic category.
                                                   </font></p>
                                                   <hr>
                                                   <h4>Dashboard: Project Semantics</h4>
                                                   <br>
                                                   <p><font size = 2>Make a selection of Wikimedia projects here and hit <i>Apply Selection</i>. The Dashboard will produce a series of charts, one per each Wikidata semantic category that is 
                                                   present in your selection of projects, and compute the relative importance (%) of each topic in the given selection and for each semantic category. Do not forget that category specific 
                                                   semantic models do not necessarily encompass the same number of topics (in fact, they rarely do); also, <i>Topic n</i> in one category is obviously not the same thing as  <i>Topic n</i> in 
                                                   some other category.
                                                   </font></p>
                                                   <hr>
                                                   <h4>Dashboard: Similarity Maps</h4>
                                                   <br>
                                                   <p><font size = 2>Upon a selection of semantic category, the Dashboard will present a 2D map which represents the similarities between the Wikimedia projects computed from the selected category\'s 
                                                   semantic model only. Here you can learn how similar or dissimilar are the sister projects in respect to their usage Wikidata items from a single semantic category.
                                                   </font></p>
                                                   
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
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_SemanticsDashboard/">WDCM Semantics</a> (current dashboard)</b><br>
                                                   <font size = "2">Detailed insights into the WDCM Taxonomy (a selection of semantic categories from Wikidata), its distributional
                                                   semantics, and the way it is used across the client projects. If you are looking for Topic Models - that&#8217;s where
                                                   they live.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_UsageDashboard/">WDCM Usage</a></b><br>
                                                   <font size = "2">Fine-grained information on Wikidata usage across client projects and project types. Cross-tabulations and similar..</font></li><br>
                                                   <li><b>WDCM Items</b><br>
                                                   <font size = "2">Fine-grained information on particular Wikidata item usage across the client projects.<b> (Under development)</b></font></li><br>
                                                   <li><b>WDCM System Technical Documentation</b><br>
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
                     HTML('<b>Wikidata Concepts Monitor :: WMDE 2017</b><br>Diffusion: <a href="https://phabricator.wikimedia.org/diffusion/AWCM/" target = "_blank">WDCM</a><br>'),
                     HTML('Contact: Goran S. Milovanovic, Data Scientist, WMDE<br>e-mail: goran.milovanovic_ext@wikimedia.de
                          <br>IRC: goransm'),
                     br(),
                     br()
                     )
            ) # - fluidRow Footer END
            
            ) # - fluidPage END
  
) # - ShinyUI END