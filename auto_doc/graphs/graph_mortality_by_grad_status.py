from ..auto_doc import TrackedGraph
from ..tables.education_highest_bc import EducationHighestBc
from ..tables.ses6 import Ses6

from dateutil.relativedelta import relativedelta

start_ymd = EducationHighestBc.education_data_start_date
end_ymd = EducationHighestBc.education_data_end_date

education_data_year_months = []
cur_date = start_ymd

while cur_date <= end_ymd:
    education_data_year_months.append(cur_date)
    cur_date += relativedelta(months=1)

"""
gr10 exams (all 3)
gr10 exams (at least one)
GPA > 75%
Communications 12 or English 12
Adult Grad or Regular Grad
Grad on time or 6 year completion
Homeschooled
Secondary Ungraded
Highest grade completed
"""

class GraphMortalityByGradStatus(TrackedGraph):
    name = 'graph_mortality_by_grad_status'

    age_ranges = [[20, 24],]

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE graph_mortality_by_grad_status ("
                "count NUMERIC, "
                "year_month TEXT, "
                "gender TEXT, "
                "died_this_year_month BOOLEAN, "
                "grad_status NUMERIC"
            ")"
        )

        for date in education_data_year_months:
            for min_age, max_age in self.age_ranges:
                cursor.execute(
                    "INSERT INTO graph_mortality_by_grad_status "
                        "SELECT "
                            "COUNT(a.study_id) AS count, "
                            "'{year}-{month}' AS year_month, "
                            "a.demographics_unduped_gender AS gender, "
                            "(DATE_PART('year', a.deaths_unduped_date_of_death) = {year} AND DATE_PART('month', a.deaths_unduped_date_of_death) = {month}) AS died_this_year_month, "
                            "a.education_ever_grad_grad AS grad_status "
                        "FROM education_highest_bc a, ses6 b "
                        "WHERE "
                            "a.study_id = b.study_id AND "
                            "a.education_highest_bc_in_possible_grad_pop = TRUE AND "
                            "a.deaths_unduped_date_of_death >= MAKE_DATE({year}, {month}, {day}) AND "
                            "SUBSTRING(b.ses6_pat, {ses_index}) != '0' AND "
                            "AGE(MAKE_TIMESTAMP({year}, {month}, {day}, 0, 0, 0), a.demographics_unduped_dob) BETWEEN INTERVAL '{min_age} YEARS' AND INTERVAL '{max_age} YEARS' "
                        "GROUP BY "
                            "year_month, "
                            "gender, "
                            "died_this_year_month, "
                            "grad_status".format(
                        year=date.year,
                        month=date.month,
                        day=date.day,
                        min_age=min_age,
                        max_age=max_age,
                        ses_index=Ses6.index_from_year_month['{year}-{month}'.format(
                            year=date.year,
                            month=str(date.month).rjust(2, '0'),
                        )]
                    )
                )

    def make_graph_product(self):
        data_frame = self.load_dataframe()
        # Use self.save_plot(name) to save and close a plot created with data_frame.plot
        # name should be just the name of the file, no path or file extension
