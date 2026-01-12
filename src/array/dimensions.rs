#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Dimensions {
    Scalar,
    Multi(Vec<Dimension>),
}

impl Dimensions {
    pub fn len(&self) -> usize {
        match self {
            Dimensions::Scalar => 1,
            Dimensions::Multi(dimensions) => dimensions.iter().map(|d| d.size).product(),
        }
    }
}

impl From<&ArchivedDimensions> for nd_arrow_array::dimensions::Dimensions {
    fn from(archived: &ArchivedDimensions) -> Self {
        match archived {
            ArchivedDimensions::Scalar => Self::Scalar,
            ArchivedDimensions::Multi(dimensions) => {
                Self::MultiDimensional(dimensions.iter().map(|d| d.into()).collect())
            }
        }
    }
}

impl From<&nd_arrow_array::dimensions::Dimensions> for Dimensions {
    fn from(nd_dimensions: &nd_arrow_array::dimensions::Dimensions) -> Self {
        match nd_dimensions {
            nd_arrow_array::dimensions::Dimensions::Scalar => Self::Scalar,
            nd_arrow_array::dimensions::Dimensions::MultiDimensional(dimensions) => {
                Self::Multi(dimensions.iter().map(|d| d.into()).collect())
            }
        }
    }
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Dimension {
    pub name: String,
    pub size: usize,
}

impl From<(String, usize)> for Dimension {
    fn from(tuple: (String, usize)) -> Self {
        Self {
            name: tuple.0,
            size: tuple.1,
        }
    }
}

impl From<(&str, usize)> for Dimension {
    fn from(tuple: (&str, usize)) -> Self {
        Self {
            name: tuple.0.to_string(),
            size: tuple.1,
        }
    }
}

impl From<&ArchivedDimension> for nd_arrow_array::dimensions::Dimension {
    fn from(archived: &ArchivedDimension) -> Self {
        Self {
            name: archived.name.to_string(),
            size: archived.size.to_native() as usize,
        }
    }
}

impl From<&nd_arrow_array::dimensions::Dimension> for Dimension {
    fn from(nd_dimension: &nd_arrow_array::dimensions::Dimension) -> Self {
        Self {
            name: nd_dimension.name.to_string(),
            size: nd_dimension.size,
        }
    }
}
