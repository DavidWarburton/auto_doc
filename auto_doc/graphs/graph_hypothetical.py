"""
Copyright 2019 Province of British Columbia

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from ..auto_doc import TrackedGraph


class GraphHypothetical(TrackedGraph):
    name = 'graph_hypothetical'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table graph_hypothetical as 
            select count(*) as diags, extract ('year' from msp_autism_servdate ) as serv_yr
            from msp_autism 
            group by 2
            order by 2;
                """)
        pass
    def make_graph_product(self):        # Use data_frame =  to load this table as a dataframe
        # and create a csv for our records.

        # Use sel to save and close a plot created with data_frame.plot
        # name should be just the name of the file, no path or file extension
        df = self.load_dataframe()
        df.plot()
        self.save_plot("graph hypothetical")
