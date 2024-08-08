use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use crate::relabel::actions::label_drop::LabelDropAction;
use crate::relabel::actions::label_keep::LabelKeepAction;
use crate::relabel::actions::keep::KeepAction;
use crate::relabel::actions::keep_equal::KeepEqualAction;
use crate::relabel::actions::keep_if_contains::KeepIfContainsAction;
use crate::relabel::actions::keep_if_equal::KeepIfEqualAction;
use crate::relabel::actions::drop::DropAction;
use crate::relabel::actions::drop_equal::DropEqualAction;
use crate::relabel::actions::drop_if_contains::DropIfContainsAction;
use crate::relabel::actions::drop_if_equal::DropIfEqualAction;
use crate::relabel::actions::hashmod::HashModAction;
use crate::relabel::actions::replace_all::ReplaceAllAction;
use crate::relabel::actions::graphite::GraphiteAction;
use crate::relabel::actions::lowercase::LowercaseAction;
use crate::relabel::actions::uppercase::UppercaseAction;
use crate::relabel::actions::replace::ReplaceAction;
use crate::relabel::actions::label_map::LabelMapAction;
use crate::relabel::actions::label_map_all::LabelMapAllAction;
use crate::storage::Label;

pub trait Action {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize);
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum RelabelActionType {
    Drop,
    DropEqual,
    DropIfContains,
    DropIfEqual,
    DropMetrics,
    Graphite,
    HashMod,
    Keep,
    KeepEqual,
    KeepIfContains,
    KeepIfEqual,
    KeepMetrics,
    Lowercase,
    LabelMap,
    LabelMapAll,
    LabelDrop,
    LabelKeep,
    #[default]
    Replace,
    ReplaceAll,
    Uppercase,
}

impl Display for RelabelActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RelabelActionType::*;
        match self {
            Graphite => write!(f, "graphite"),
            Replace => write!(f, "replace"),
            ReplaceAll => write!(f, "replace_all"),
            KeepIfEqual => write!(f, "keep_if_equal"),
            DropIfEqual => write!(f, "drop_if_equal"),
            KeepEqual => write!(f, "keepequal"),
            DropEqual => write!(f, "dropequal"),
            Keep => write!(f, "keep"),
            Drop => write!(f, "drop"),
            DropIfContains => write!(f, "drop_if_contains"),
            DropMetrics => write!(f, "drop_metrics"),
            HashMod => write!(f, "hashmod"),
            KeepMetrics => write!(f, "keep_metrics"),
            Uppercase => write!(f, "uppercase"),
            Lowercase => write!(f, "lowercase"),
            LabelMap => write!(f, "labelmap"),
            LabelMapAll => write!(f, "labelmap_all"),
            LabelDrop => write!(f, "labeldrop"),
            LabelKeep => write!(f, "labelkeep"),
            KeepIfContains => write!(f, "keep_if_contains"),
        }
    }
}

impl FromStr for RelabelActionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use RelabelActionType::*;
        match s.to_lowercase().as_str() {
            "graphite" => Ok(Graphite),
            "replace" => Ok(Replace),
            "replace_all" => Ok(ReplaceAll),
            "keep_if_equal" => Ok(KeepIfEqual),
            "drop" => Ok(Drop),
            "drop_equal" | "dropequal" => Ok(DropEqual),
            "drop_if_equal" => Ok(DropIfEqual),
            "drop_if_contains" => Ok(DropIfContains),
            "drop_metrics" => Ok(DropMetrics),
            "keep_equal" | "keepequal" => Ok(KeepEqual),
            "keep" => Ok(Keep),
            "hashmod" => Ok(HashMod),
            "keep_metrics" => Ok(KeepMetrics),
            "keep_if_contains" => Ok(KeepIfContains),
            "lowercase" => Ok(Lowercase),
            "labelmap" => Ok(LabelMap),
            "labelmap_all" => Ok(LabelMapAll),
            "labeldrop" | "label_drop" => Ok(LabelDrop),
            "labelkeep" | "label_keep" => Ok(LabelKeep),
            "uppercase" => Ok(Uppercase),
            _ => Err(format!("unknown action: {}", s)),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActionRelabel {
    Keep(KeepAction),
    KeepEqual(KeepEqualAction),
    KeepIfContains(KeepIfContainsAction),
    KeepIfEqual(KeepIfEqualAction),
    Drop(DropAction),
    DropEqual(DropEqualAction),
    DropIfContains(DropIfContainsAction),
    DropIfEqual(DropIfEqualAction),
    HashMod(HashModAction),
    ReplaceAll(ReplaceAllAction),
    Graphite(GraphiteAction),
    LabelDrop(LabelDropAction),
    Lowercase(LowercaseAction),
    Uppercase(UppercaseAction),
    Replace(ReplaceAction),
    LabelKeep(LabelKeepAction),
    LabelMap(LabelMapAction),
    LabelMapAll(LabelMapAllAction),
}

impl Action for ActionRelabel {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        match self {
            ActionRelabel::Keep(action) => action.apply(labels, labels_offset),
            ActionRelabel::KeepEqual(action) => action.apply(labels, labels_offset),
            ActionRelabel::KeepIfContains(action) => action.apply(labels, labels_offset),
            ActionRelabel::KeepIfEqual(action) => action.apply(labels, labels_offset),
            ActionRelabel::Drop(action) => action.apply(labels, labels_offset),
            ActionRelabel::DropEqual(action) => action.apply(labels, labels_offset),
            ActionRelabel::DropIfContains(action) => action.apply(labels, labels_offset),
            ActionRelabel::DropIfEqual(action) => action.apply(labels, labels_offset),
            ActionRelabel::HashMod(action) => action.apply(labels, labels_offset),
            ActionRelabel::ReplaceAll(action) => action.apply(labels, labels_offset),
            ActionRelabel::Graphite(action) => action.apply(labels, labels_offset),
            ActionRelabel::LabelDrop(action) => action.apply(labels, labels_offset),
            ActionRelabel::Lowercase(action) => action.apply(labels, labels_offset),
            ActionRelabel::Uppercase(action) => action.apply(labels, labels_offset),
            ActionRelabel::Replace(action) => action.apply(labels, labels_offset),
            ActionRelabel::LabelKeep(action) => action.apply(labels, labels_offset),
            ActionRelabel::LabelMap(action) => action.apply(labels, labels_offset),
            ActionRelabel::LabelMapAll(action) => action.apply(labels, labels_offset),
        }
    }
}

mod drop_equal;
mod drop_if_contains;
mod drop_if_equal;
mod keep_if_equal;
mod keep;
mod keep_equal;
mod keep_if_contains;
mod hashmod;
mod replace_all;
mod graphite;
mod label_drop;
mod lowercase;
mod uppercase;
mod drop;
mod label_map;
mod label_map_all;
mod replace;
mod label_keep;
mod utils;
