use super::query_data::QueryData;

pub struct CreateViewData {
    view_name: String,
    query: QueryData,
}

impl CreateViewData {
    pub fn new(view_name: String, query: QueryData) -> Self {
        CreateViewData { view_name, query }
    }

    pub fn view_name(&self) -> &str {
        &self.view_name
    }

    pub fn view_def(&self) -> &QueryData {
        &self.query
    }
}
